package app.l2nx.gs.runtime.sync.engine;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.runtime.sync.engine.publish.KafkaSender;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EntityTickLoopTest {

    @Test
    void tick_shouldEmitCreatedForNewPks_andUpdatedForChangedHashes() {
        Map<Long, Long> hashes = new HashMap<Long, Long>();
        hashes.put(1L, 100L);
        hashes.put(2L, 200L);
        StubMapping mapping = new StubMapping(hashes);
        CapturingSender sender = new CapturingSender();
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            // First tick — both pks new → CREATED.
            loop.tick();

            assertEquals(2, sender.captured.size());
            assertOpsContains(sender.captured, "CREATED", 2);

            sender.captured.clear();

            // Second tick — same hashes → no events.
            loop.tick();
            assertEquals(0, sender.captured.size());

            // Third tick — change pk=1 hash → UPDATED only for pk=1.
            mapping.hashesByPk.put(1L, 999L);
            loop.tick();
            assertEquals(1, sender.captured.size());
            assertOpsContains(sender.captured, "UPDATED", 1);
            assertEquals(1L, sender.captured.get(0).pk);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldDropGonePks_withoutEmittingTombstone() {
        Map<Long, Long> hashes = new HashMap<Long, Long>();
        hashes.put(1L, 100L);
        hashes.put(2L, 200L);
        StubMapping mapping = new StubMapping(hashes);
        CapturingSender sender = new CapturingSender();
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);
            loop.tick(); // both CREATED
            sender.captured.clear();

            // pk=2 disappears.
            mapping.hashesByPk.remove(2L);
            loop.tick();

            // No DELETED — runtime-sync owns no tombstones.
            assertEquals(0, sender.captured.size());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldDegrade_whenTopicMissing() {
        StubMapping mapping = new StubMapping(Collections.singletonMap(1L, 100L));
        CapturingSender sender = new CapturingSender();
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(mapping,
                    e -> null,
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();

            assertEquals(0, sender.captured.size());
            assertEquals(1, tracker.currentStatuses().size());
            assertEquals(app.l2nx.gs.adapter.api.kafka.ops.EntityState.DEGRADED,
                    tracker.currentStatuses().get(0).getState());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldDegrade_whenSnapshotThrows() {
        RuntimeEntityMapping<String> throwing = new RuntimeEntityMapping<String>() {
            @Override
            public String entityName() {
                return "character";
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
            }

            @Override
            public Iterable<RuntimeRow<String>> snapshot() {
                throw new RuntimeException("boom");
            }

            @Override
            public long hash(String dto) {
                return 0L;
            }
        };
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(throwing,
                    e -> "topic.character",
                    new SyncEventPublisher((t, k, v, cb) -> {
                    }),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();

            assertEquals(app.l2nx.gs.adapter.api.kafka.ops.EntityState.DEGRADED,
                    tracker.currentStatuses().get(0).getState());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void resolveTopicFromContext_shouldReadRuntimeNamespace() {
        TopicResolver runtimeOnly = TopicResolver.fromSnapshot(
                Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"));

        assertEquals("bohpts.gs.sync.runtime.character", runtimeOnly.resolveTopic("character"));
        assertNull(runtimeOnly.resolveTopic("clan"));
    }

    private static void assertOpsContains(List<CapturedSend> captured, String op, int expectedCount) {
        int count = 0;
        for (CapturedSend c : captured) {
            if (op.equals(c.op)) count++;
        }
        assertEquals(expectedCount, count, "expected " + expectedCount + " " + op + " events");
    }

    private static final class StubMapping implements RuntimeEntityMapping<String> {
        private final Map<Long, Long> hashesByPk;

        StubMapping(Map<Long, Long> hashesByPk) {
            this.hashesByPk = new HashMap<Long, Long>(hashesByPk);
        }

        @Override
        public String entityName() {
            return "character";
        }

        @Override
        public Class<String> dtoType() {
            return String.class;
        }

        @Override
        public Iterable<RuntimeRow<String>> snapshot() {
            List<RuntimeRow<String>> rows = new ArrayList<RuntimeRow<String>>();
            for (Map.Entry<Long, Long> e : hashesByPk.entrySet()) {
                rows.add(new RuntimeRow<String>(e.getKey(), "dto-" + e.getKey()));
            }
            return rows;
        }

        @Override
        public long hash(String dto) {
            // Hash deterministically derived from the DTO suffix matched to current map.
            long pk = Long.parseLong(dto.substring("dto-".length()));
            Long h = hashesByPk.get(pk);
            return h == null ? 0L : h;
        }
    }

    private static final class CapturedSend {
        final String topic;
        final long pk;
        final String op;

        CapturedSend(String topic, long pk, String op) {
            this.topic = topic;
            this.pk = pk;
            this.op = op;
        }
    }

    private static final class CapturingSender implements KafkaSender {
        final List<CapturedSend> captured = new ArrayList<CapturedSend>();

        @Override
        public void send(String topic, byte[] key, Object value,
                         org.apache.kafka.clients.producer.Callback callback) {
            SyncEvent<?> event = (SyncEvent<?>) value;
            captured.add(new CapturedSend(topic, event.getPk(), event.getOp()));
            // Synchronous ack — test wants the publish to count as successful.
            callback.onCompletion(new RecordMetadata(
                    new TopicPartition(topic, 0), 0L, 0,
                    RecordBatch.NO_TIMESTAMP, 0, 0), null);
        }
    }
}
