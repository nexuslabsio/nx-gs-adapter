package app.l2nx.gs.gd.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Drives the {@code gearscore} entity through the production {@link GameDataSyncModule#defaultDescriptors()}
 * registry. The module resolves providers via {@link java.util.ServiceLoader}, so a real test impl on the
 * classpath ({@link TestGearScoreRulesetProvider}, registered under {@code META-INF/services}) is what proves
 * the descriptor is registered, resolves, and publishes — the exact wiring the production fix restores.
 */
class GearScoreDescriptorTest {

    private static final String TOPIC = "kbt.gd.sync.gearscore";

    private List<GameDataSyncEvent<?>> recorded;
    private GameDataSyncModule module;

    @BeforeEach
    void setUp() {
        recorded = new ArrayList<GameDataSyncEvent<?>>();
        GameDataSender sender = new RecordingSender(recorded);
        module = new GameDataSyncModule(GameDataSyncModule.defaultDescriptors(), sender);
    }

    @AfterEach
    void tearDown() {
        TestGearScoreRulesetProvider.snapshot = Optional.empty();
    }

    private void connectAndSnapshot() {
        SyncTopics topics = SyncTopics.builder()
                .gd(Collections.singletonMap("gearscore", TOPIC))
                .build();
        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("kbt")
                .serverId(UUID.randomUUID())
                .serverSlug("kbt-x1")
                .serverName("x1")
                .adapterVersion("test")
                .syncTopics(topics)
                .build();
        module.onConnect(ctx);
        // ctx.io() defaults to a direct-run executor, so start()'s initial snapshot
        // runs synchronously on this thread — no await needed.
        module.start();
    }

    private List<GameDataSyncEvent<?>> gearscoreEvents() {
        List<GameDataSyncEvent<?>> out = new ArrayList<GameDataSyncEvent<?>>();
        for (GameDataSyncEvent<?> e : recorded) {
            if ("gearscore".equals(e.getEntityName())) {
                out.add(e);
            }
        }
        return out;
    }

    @Nested
    class Resolve {

        @Test
        void onConnect_shouldIncludeGearscore_inResyncEntities() {
            TestGearScoreRulesetProvider.snapshot =
                    Optional.of(GearScoreRuleset.builder().enabled(true).build());

            connectAndSnapshot();

            List<EntityStats> entities =
                    module.currentStatus().getStats().getEntities().orElseGet(Collections::emptyList);
            List<String> resyncEntities = new ArrayList<String>();
            for (EntityStats s : entities) {
                resyncEntities.add(s.getName());
            }
            assertTrue(
                    resyncEntities.contains("gearscore"),
                    "gearscore must appear among the active entities (resync path)");
        }
    }

    @Nested
    class PublishSnapshot {

        @Test
        void start_shouldPublishOneRowBurst_whenRulesetPresent() {
            GearScoreRuleset ruleset = GearScoreRuleset.builder().enabled(true).build();
            TestGearScoreRulesetProvider.snapshot = Optional.of(ruleset);

            connectAndSnapshot();

            List<GameDataSyncEvent<?>> events = gearscoreEvents();
            assertEquals(2, events.size(), "one UPSERT + one SNAPSHOT_COMPLETE");

            GameDataSyncEvent<?> upsert = events.get(0);
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, upsert.getOp());
            assertEquals(Long.valueOf(0L), upsert.getPk(), "singleton uses a constant pk");
            assertEquals(ruleset, upsert.getPayload());

            GameDataSyncEvent<?> complete = events.get(1);
            assertEquals(GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE, complete.getOp());
            assertEquals(Integer.valueOf(1), complete.getCount());
            assertEquals(upsert.getSyncId(), complete.getSyncId(), "burst shares one syncId");
        }

        @Test
        void start_shouldPublishCountZeroSnapshot_whenRulesetAbsent() {
            TestGearScoreRulesetProvider.snapshot = Optional.empty();

            connectAndSnapshot();

            List<GameDataSyncEvent<?>> events = gearscoreEvents();
            assertEquals(1, events.size(), "empty Optional → no UPSERT, only the marker");

            GameDataSyncEvent<?> complete = events.get(0);
            assertEquals(GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE, complete.getOp());
            assertEquals(
                    Integer.valueOf(0),
                    complete.getCount(),
                    "count=0 marker drives the consumer to delete the disabled singleton");
            assertNull(complete.getPk(), "the marker carries no row key");
        }
    }

    private static final class RecordingSender implements GameDataSender {

        private final List<GameDataSyncEvent<?>> sink;

        RecordingSender(List<GameDataSyncEvent<?>> sink) {
            this.sink = sink;
        }

        @Override
        public void send(ProducerRecord<byte[], Object> record, Callback callback) {
            sink.add((GameDataSyncEvent<?>) record.value());
            callback.onCompletion(null, null);
        }
    }
}
