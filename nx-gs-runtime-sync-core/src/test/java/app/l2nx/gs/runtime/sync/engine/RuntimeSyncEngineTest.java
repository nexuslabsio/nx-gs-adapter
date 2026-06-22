package app.l2nx.gs.runtime.sync.engine;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.runtime.sync.engine.publish.KafkaSender;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

class RuntimeSyncEngineTest {

    @Test
    void start_shouldReject_duplicateEntityName() {
        StubMapping a = new StubMapping("character", Collections.emptyMap());
        StubMapping b = new StubMapping("character", Collections.emptyMap());

        RuntimeSyncEngine engine = new RuntimeSyncEngine(
                Arrays.<RuntimeEntityMapping<?>>asList(a, b),
                e -> "topic",
                new SyncEventPublisher((t, k, v, cb) -> {}),
                new EntityStatsTracker(),
                new EngineConfig(10, 5));

        IllegalStateException ex = assertThrows(IllegalStateException.class, engine::start);
        assertTrue(ex.getMessage().contains("Duplicate"));
    }

    @Test
    void start_shouldReject_nullOrBlankEntityName() {
        StubMapping blank = new StubMapping("  ", Collections.emptyMap());

        RuntimeSyncEngine engine = new RuntimeSyncEngine(
                Collections.<RuntimeEntityMapping<?>>singletonList(blank),
                e -> "topic",
                new SyncEventPublisher((t, k, v, cb) -> {}),
                new EntityStatsTracker(),
                new EngineConfig(10, 5));

        assertThrows(IllegalStateException.class, engine::start);
    }

    @Test
    void start_shouldDispatchTicks_forMultipleEntitiesOnSharedPool() throws Exception {
        // 2s tick interval — fast enough for one tick to fire inside the test budget.
        Map<Long, Long> emptyHashes = Collections.emptyMap();
        StubMapping a = new StubMapping("character", emptyHashes);
        StubMapping b = new StubMapping("party", emptyHashes);
        a.snapshotLatch = new CountDownLatch(1);
        b.snapshotLatch = new CountDownLatch(1);

        EntityStatsTracker tracker = new EntityStatsTracker();
        Map<String, String> topics = new HashMap<String, String>();
        topics.put("character", "t.character");
        topics.put("party", "t.party");

        RuntimeSyncEngine engine = new RuntimeSyncEngine(
                Arrays.<RuntimeEntityMapping<?>>asList(a, b),
                TopicResolver.fromSnapshot(topics),
                new SyncEventPublisher(silentSender()),
                tracker,
                new EngineConfig(1, 1));
        try {
            engine.start();
            assertTrue(
                    a.snapshotLatch.await(5L, TimeUnit.SECONDS),
                    "entity 'character' tick should have fired on the shared pool");
            assertTrue(
                    b.snapshotLatch.await(5L, TimeUnit.SECONDS),
                    "entity 'party' tick should have fired on the shared pool");
            Set<String> threadNames = new HashSet<String>();
            threadNames.add(a.lastThreadName.get());
            threadNames.add(b.lastThreadName.get());
            for (String name : threadNames) {
                assertTrue(
                        name != null && name.startsWith("nx-runtime-sync-pool-"),
                        "ticks must run on shared-pool threads, saw: " + name);
            }
        } finally {
            engine.stop();
        }
    }

    @Test
    void tickGuard_shouldSkip_whenPreviousTickStillRunning() throws Exception {
        // First tick blocks in snapshot(); a second scheduled tick must skip with a WARN
        // (verified indirectly: the snapshot counter does not increment).
        CountDownLatch holdInside = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger snapshotCalls = new AtomicInteger(0);
        StubMapping blocking = new StubMapping("character", Collections.emptyMap());
        blocking.beforeSnapshot = () -> {
            snapshotCalls.incrementAndGet();
            entered.countDown();
            try {
                holdInside.await(5L, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        };

        EntityStatsTracker tracker = new EntityStatsTracker();
        RuntimeSyncEngine engine = new RuntimeSyncEngine(
                Collections.<RuntimeEntityMapping<?>>singletonList(blocking),
                e -> "t.character",
                new SyncEventPublisher(silentSender()),
                tracker,
                new EngineConfig(1, 1));
        try {
            engine.start();
            assertTrue(entered.await(5L, TimeUnit.SECONDS), "first tick must have entered snapshot()");
            // Let the scheduler attempt at least one more dispatch while we hold tick #1.
            Thread.sleep(1500L);
            assertEquals(1, snapshotCalls.get(), "overlapping scheduled tick must be skipped by the tick guard");
        } finally {
            holdInside.countDown();
            engine.stop();
        }
    }

    @Test
    void stop_shouldCleanShutdown_whileTickMidWalk() throws Exception {
        // Sender never invokes callback → publish future hangs → walk-in-flight enters
        // deadline wait; stop() must interrupt it and terminate within shutdownTimeoutSeconds.
        Map<Long, Long> hashes = new HashMap<Long, Long>();
        hashes.put(1L, 100L);
        StubMapping mapping = new StubMapping("character", hashes);

        KafkaSender silent = (topic, key, value, callback) -> {
            /* never acks */
        };
        EntityStatsTracker tracker = new EntityStatsTracker();
        RuntimeSyncEngine engine = new RuntimeSyncEngine(
                Collections.<RuntimeEntityMapping<?>>singletonList(mapping),
                e -> "t.character",
                new SyncEventPublisher(silent),
                tracker,
                new EngineConfig(1, 30));
        mapping.snapshotLatch = new CountDownLatch(1);
        engine.start();
        assertTrue(mapping.snapshotLatch.await(5L, TimeUnit.SECONDS), "first tick must enter snapshot before stop()");
        // Give the loop a moment to advance into the ack-walk.
        Thread.sleep(500L);

        long t0 = System.nanoTime();
        engine.stop();
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        // publish-flush-seconds=30 → awaitTermination budget is 31s; clean stop should
        // interrupt the futures and return immediately, not wait the full budget.
        assertTrue(
                elapsedMs < 5_000L, "stop() should not wait the full publish-flush budget (took " + elapsedMs + "ms)");
    }

    private static KafkaSender silentSender() {
        return (topic, key, value, callback) -> callback.onCompletion(
                new RecordMetadata(new TopicPartition(topic, 0), 0L, 0, RecordBatch.NO_TIMESTAMP, 0, 0), null);
    }

    private static final class StubMapping implements RuntimeEntityMapping<String> {
        private final String name;
        private final Map<Long, Long> hashesByPk;
        final AtomicReference<String> lastThreadName = new AtomicReference<String>();
        volatile CountDownLatch snapshotLatch;
        volatile Runnable beforeSnapshot;

        StubMapping(String name, Map<Long, Long> hashesByPk) {
            this.name = name;
            this.hashesByPk = new HashMap<Long, Long>(hashesByPk);
        }

        @Override
        public String entityName() {
            return name;
        }

        @Override
        public Class<String> dtoType() {
            return String.class;
        }

        @Override
        public Iterable<RuntimeRow<String>> snapshot() {
            lastThreadName.set(Thread.currentThread().getName());
            Runnable hook = beforeSnapshot;
            if (hook != null) {
                hook.run();
            }
            CountDownLatch latch = snapshotLatch;
            if (latch != null) {
                latch.countDown();
            }
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
}
