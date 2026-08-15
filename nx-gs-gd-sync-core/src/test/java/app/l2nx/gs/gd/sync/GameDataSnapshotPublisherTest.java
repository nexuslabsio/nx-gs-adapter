package app.l2nx.gs.gd.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GameDataSnapshotPublisherTest {

    private static final String ENTITY = "itemtemplate";
    private static final String TOPIC = "kbt.gd.sync.itemtemplate";
    private static final long GRACE_MS = 1000L;
    private static final ToLongFunction<Long> PK_OF = value -> value;

    private final List<GameDataSyncEvent<?>> sent = new ArrayList<GameDataSyncEvent<?>>();
    private final AtomicLong now = new AtomicLong(0L);
    private GameDataSnapshotPublisher publisher;

    @BeforeEach
    void setUp() {
        GameDataSender sender = new GameDataSender() {
            @Override
            public void send(ProducerRecord<byte[], Object> record, Callback callback) {
                sent.add((GameDataSyncEvent<?>) record.value());
                callback.onCompletion(null, null);
            }
        };
        publisher = new GameDataSnapshotPublisher(sender, GRACE_MS, now::get);
    }

    private GameDataSnapshotPublisher.Result publishNull() {
        return publisher.publishSnapshot(ENTITY, null, PK_OF, UUID.randomUUID(), TOPIC);
    }

    private GameDataSnapshotPublisher.Result publishItems(List<Long> items) {
        return publisher.publishSnapshot(ENTITY, items, PK_OF, UUID.randomUUID(), TOPIC);
    }

    @Nested
    class NullSnapshot {

        @Test
        void publishSnapshot_shouldReturnNull_whenItemsNull() {
            assertNull(publishNull());
        }

        @Test
        void publishSnapshot_shouldSendNothing_whenItemsNull() {
            publishNull();

            assertTrue(sent.isEmpty(), "a null snapshot must not reach the sender — no SNAPSHOT_COMPLETE marker");
        }

        @ParameterizedTest(name = "elapsedMs={0}")
        @ValueSource(longs = {0L, GRACE_MS - 1L})
        void publishSnapshot_shouldStayQuiet_whileNullsRepeatInsideGraceWindow(long elapsedMs) {
            publishNull();
            now.set(elapsedMs);

            GameDataSnapshotPublisher.Result result = publishNull();

            assertNull(result);
            assertTrue(sent.isEmpty());
        }

        @Test
        void publishSnapshot_shouldStayQuiet_onceGraceWindowElapses() {
            publishNull();
            now.set(GRACE_MS);

            // The grace window escalates the WARN to an ERROR log, but that is a severity
            // decision only — the publish contract (nothing sent, null returned) is unchanged.
            GameDataSnapshotPublisher.Result result = publishNull();

            assertNull(result);
            assertTrue(sent.isEmpty());
        }

        @Test
        void publishSnapshot_shouldStayQuiet_forObservationsAfterEscalation() {
            publishNull();
            now.set(GRACE_MS);
            publishNull();

            now.set(GRACE_MS + 500L);
            GameDataSnapshotPublisher.Result result = publishNull();

            assertNull(result);
            assertTrue(sent.isEmpty());
        }
    }

    @Nested
    class SuccessfulPublish {

        @Test
        void publishSnapshot_shouldPublishFullBurst_whenItemsPresent() {
            GameDataSnapshotPublisher.Result result = publishItems(Arrays.asList(1L, 2L));

            assertEquals(2, result.count());
            assertTrue(result.complete());
            assertEquals(3, sent.size(), "2 UPSERT + 1 SNAPSHOT_COMPLETE");
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, sent.get(0).getOp());
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, sent.get(1).getOp());
            assertEquals(
                    GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE, sent.get(2).getOp());
            assertEquals(Integer.valueOf(2), sent.get(2).getCount());
        }

        @Test
        void publishSnapshot_shouldPublishFullBurst_afterEscalatedNulls() {
            publishNull();
            now.set(GRACE_MS);
            publishNull();
            sent.clear();

            GameDataSnapshotPublisher.Result result = publishItems(Collections.singletonList(1L));

            assertEquals(1, result.count());
            assertTrue(result.complete());
            assertEquals(
                    2,
                    sent.size(),
                    "1 UPSERT + 1 SNAPSHOT_COMPLETE — a real snapshot still publishes fine after nulls");
        }

        @Test
        void publishSnapshot_shouldTreatNextNullAsFresh_afterASuccessfulPublish() {
            publishItems(Collections.singletonList(1L));
            sent.clear();

            GameDataSnapshotPublisher.Result result = publishNull();

            assertNull(
                    result, "a successful publish resets the per-entity tracker, but the null contract is unchanged");
            assertTrue(sent.isEmpty());
        }
    }

    @Nested
    class EscalationWiring {

        private static final String OTHER_ENTITY = "npctemplate";

        @Test
        void trackerFor_shouldReportFirst_onFirstNullPublish() {
            publishNull();

            assertEquals(
                    EscalationTracker.Stage.FIRST, publisher.trackerFor(ENTITY).lastStage());
        }

        @Test
        void trackerFor_shouldReportRepeat_forNullInsideGraceWindow() {
            publishNull();
            now.set(GRACE_MS - 1L);

            publishNull();

            assertEquals(
                    EscalationTracker.Stage.REPEAT, publisher.trackerFor(ENTITY).lastStage());
        }

        @Test
        void trackerFor_shouldReportEscalated_forNullAtOrAfterGraceWindow() {
            publishNull();
            now.set(GRACE_MS);

            publishNull();

            assertEquals(
                    EscalationTracker.Stage.ESCALATED,
                    publisher.trackerFor(ENTITY).lastStage());
        }

        @Test
        void trackerFor_shouldReportSilent_forNullsAfterEscalation() {
            publishNull();
            now.set(GRACE_MS);
            publishNull();

            now.set(GRACE_MS + 500L);
            publishNull();

            assertEquals(
                    EscalationTracker.Stage.SILENT, publisher.trackerFor(ENTITY).lastStage());
        }

        @Test
        void trackerFor_shouldKeyPerEntity_soOneEntityEscalatingLeavesAnotherAtFirst() {
            publishNull();
            now.set(GRACE_MS);
            publishNull();
            assertEquals(
                    EscalationTracker.Stage.ESCALATED,
                    publisher.trackerFor(ENTITY).lastStage());

            publisher.publishSnapshot(OTHER_ENTITY, null, PK_OF, UUID.randomUUID(), TOPIC);

            assertEquals(
                    EscalationTracker.Stage.FIRST,
                    publisher.trackerFor(OTHER_ENTITY).lastStage());
            assertEquals(
                    EscalationTracker.Stage.ESCALATED,
                    publisher.trackerFor(ENTITY).lastStage(),
                    "another entity's first null must not disturb an already-escalated entity");
        }

        @Test
        void trackerFor_shouldResetToFirst_afterASuccessfulPublish() {
            publishNull();
            now.set(GRACE_MS);
            publishNull();
            assertEquals(
                    EscalationTracker.Stage.ESCALATED,
                    publisher.trackerFor(ENTITY).lastStage());

            publishItems(Collections.singletonList(1L));
            publishNull();

            assertEquals(
                    EscalationTracker.Stage.FIRST, publisher.trackerFor(ENTITY).lastStage());
        }
    }
}
