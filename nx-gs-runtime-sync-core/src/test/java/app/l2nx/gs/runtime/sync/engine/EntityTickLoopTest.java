package app.l2nx.gs.runtime.sync.engine;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.runtime.sync.engine.publish.KafkaSender;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

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
            EntityTickLoop loop = new EntityTickLoop(
                    mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();

            assertEquals(2, sender.captured.size());
            assertOpsContains(sender.captured, "CREATED", 2);

            sender.captured.clear();

            loop.tick();
            assertEquals(0, sender.captured.size());

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
            EntityTickLoop loop = new EntityTickLoop(
                    mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);
            loop.tick();
            sender.captured.clear();

            mapping.hashesByPk.remove(2L);
            loop.tick();

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
            EntityTickLoop loop = new EntityTickLoop(
                    mapping, e -> null, new SyncEventPublisher(sender), tracker, new EngineConfig(10, 5), scheduler);

            loop.tick();

            assertEquals(0, sender.captured.size());
            assertEquals(1, tracker.currentStatuses().size());
            assertEquals(EntityState.DEGRADED, tracker.currentStatuses().get(0).getState());
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
            EntityTickLoop loop = new EntityTickLoop(
                    throwing,
                    e -> "topic.character",
                    new SyncEventPublisher((t, k, v, cb) -> {}),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();

            assertEquals(EntityState.DEGRADED, tracker.currentStatuses().get(0).getState());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldDegrade_whenSnapshotIterationThrowsCme() {
        RuntimeEntityMapping<String> cmeMapping = new RuntimeEntityMapping<String>() {
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
                return () -> new Iterator<RuntimeRow<String>>() {
                    private boolean served;

                    @Override
                    public boolean hasNext() {
                        return !served;
                    }

                    @Override
                    public RuntimeRow<String> next() {
                        served = true;
                        // Faulty provider: lies about hasNext, then throws CME mid-stream.
                        throw new java.util.ConcurrentModificationException("live view mutated");
                    }
                };
            }

            @Override
            public long hash(String dto) {
                return 0L;
            }
        };
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(
                    cmeMapping,
                    e -> "topic.character",
                    new SyncEventPublisher((t, k, v, cb) -> {}),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();

            assertEquals(EntityState.DEGRADED, tracker.currentStatuses().get(0).getState());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldRecordDegraded_whenPublishFutureFails() {
        StubMapping mapping = new StubMapping(Collections.singletonMap(1L, 100L));
        KafkaSender failing =
                (topic, key, value, callback) -> callback.onCompletion(null, new RuntimeException("publish boom"));
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(
                    mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(failing),
                    tracker,
                    new EngineConfig(10, 1),
                    scheduler);

            loop.tick();

            assertEquals(EntityState.DEGRADED, tracker.currentStatuses().get(0).getState());
            assertEquals(1L, tracker.failedAcks(mapping.entityName()));
            assertEquals(0L, tracker.timedOutAcks(mapping.entityName()));
            // pk replayed next tick — snapshot must not have advanced for it.
            assertTrue(
                    loop.currentSnapshotKeysForTesting().isEmpty(),
                    "failed-publish PK must not advance into the next snapshot");
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void tick_shouldClassifyZeroHashAsUnchanged() {
        Map<Long, Long> hashes = new HashMap<Long, Long>();
        hashes.put(42L, 0L);
        StubMapping mapping = new StubMapping(hashes);
        CapturingSender sender = new CapturingSender();
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            EntityTickLoop loop = new EntityTickLoop(
                    mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 5),
                    scheduler);

            loop.tick();
            assertEquals(1, sender.captured.size());
            assertOpsContains(sender.captured, "CREATED", 1);

            sender.captured.clear();
            loop.tick();
            assertEquals(0, sender.captured.size(), "hash=0 must round-trip as unchanged — no spurious UPDATED");
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void walkInFlight_shouldDrainAlreadyDoneFuturesCheaply() {
        // Synchronous ack — every publish completes before ack-walk starts.
        StubMapping mapping = new StubMapping(Collections.singletonMap(1L, 100L));
        CapturingSender sender = new CapturingSender();
        EntityStatsTracker tracker = new EntityStatsTracker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            // publish-flush-seconds = 0 would normally block forever for pending futures;
            // since the sender ack'd synchronously, the drain-done pass must classify
            // everything without ever touching the timeout path.
            EntityTickLoop loop = new EntityTickLoop(
                    mapping,
                    e -> "topic.character",
                    new SyncEventPublisher(sender),
                    tracker,
                    new EngineConfig(10, 1),
                    scheduler);

            long t0 = System.nanoTime();
            loop.tick();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            assertEquals(EntityState.HEALTHY, tracker.currentStatuses().get(0).getState());
            assertTrue(
                    elapsedMs < 500L,
                    "synchronous-ack tick should not block on the publish-flush deadline (took " + elapsedMs + "ms)");
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void resolveTopicFromContext_shouldReadRuntimeNamespace() {
        TopicResolver runtimeOnly =
                TopicResolver.fromSnapshot(Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"));

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
        public void send(String topic, byte[] key, Object value, Callback callback) {
            SyncEvent<?> event = (SyncEvent<?>) value;
            captured.add(new CapturedSend(topic, event.getPk(), event.getOp()));
            callback.onCompletion(
                    new RecordMetadata(new TopicPartition(topic, 0), 0L, 0, RecordBatch.NO_TIMESTAMP, 0, 0), null);
        }
    }
}
