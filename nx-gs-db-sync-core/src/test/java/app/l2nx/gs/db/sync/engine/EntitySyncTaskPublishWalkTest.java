package app.l2nx.gs.db.sync.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import it.unimi.dsi.fastutil.longs.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Publish-walk seam: created/updated/deleted advance the snapshot, failed and
 * deadline-pending publishes are counted into tally slots 3/4 (the
 * force-resync fully-successful gate) and leave the snapshot untouched.
 */
class EntitySyncTaskPublishWalkTest {

    private static final RecordMetadata META = new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);

    private final SnapshotStore snapshot = new SnapshotStore();

    @Test
    void walkInFlightAndAdvance_shouldCountFailedAndPendingPublishes_andAdvanceOnlyAcked() {
        EntitySyncTask task = task(snapshot);

        Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        inFlight.put(1L, CompletableFuture.completedFuture(META));
        CompletableFuture<RecordMetadata> failed = new CompletableFuture<RecordMetadata>();
        failed.completeExceptionally(new RuntimeException("publish boom"));
        inFlight.put(2L, failed);
        inFlight.put(3L, new CompletableFuture<RecordMetadata>()); // never completes

        Long2IntMap pendingCrcAdvance = new Long2IntOpenHashMap();
        ((Long2IntOpenHashMap) pendingCrcAdvance).defaultReturnValue(Phase1Hasher.MISSING_HASH);
        pendingCrcAdvance.put(1L, 111);
        pendingCrcAdvance.put(2L, 222);
        pendingCrcAdvance.put(3L, 333);
        LongSet pendingCreates = new LongOpenHashSet();
        pendingCreates.add(1L);
        pendingCreates.add(2L);
        pendingCreates.add(3L);

        long[] tally =
                task.walkInFlightAndAdvance("clan", inFlight, pendingCrcAdvance, pendingCreates, new LongOpenHashSet());

        assertEquals(1L, tally[0], "one acked create");
        assertEquals(0L, tally[1]);
        assertEquals(0L, tally[2]);
        assertEquals(1L, tally[3], "one failed publish");
        assertEquals(1L, tally[4], "one publish pending past the flush deadline");
        assertEquals(111, snapshot.getCrc("clan", 1L));
        assertFalse(snapshot.containsCrc("clan", 2L), "failed publish must not advance the snapshot");
        assertFalse(snapshot.containsCrc("clan", 3L), "pending publish must not advance the snapshot");
    }

    @Test
    void walkInFlightAndAdvance_shouldReportZeroFailedAndPending_whenAllAcked() {
        EntitySyncTask task = task(snapshot);

        Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        inFlight.put(1L, CompletableFuture.completedFuture(META));
        inFlight.put(2L, CompletableFuture.completedFuture(META));

        Long2IntMap pendingCrcAdvance = new Long2IntOpenHashMap();
        ((Long2IntOpenHashMap) pendingCrcAdvance).defaultReturnValue(Phase1Hasher.MISSING_HASH);
        pendingCrcAdvance.put(1L, 111);
        LongSet pendingCreates = new LongOpenHashSet();
        pendingCreates.add(1L);
        LongSet pendingDeletes = new LongOpenHashSet();
        pendingDeletes.add(2L);
        snapshot.putCrc("clan", 2L, 200);

        long[] tally = task.walkInFlightAndAdvance("clan", inFlight, pendingCrcAdvance, pendingCreates, pendingDeletes);

        assertEquals(1L, tally[0]);
        assertEquals(0L, tally[1]);
        assertEquals(1L, tally[2]);
        assertEquals(0L, tally[3]);
        assertEquals(0L, tally[4]);
        assertEquals(111, snapshot.getCrc("clan", 1L));
        assertFalse(snapshot.containsCrc("clan", 2L), "acked delete must drop the snapshot entry");
    }

    private static EntitySyncTask task(SnapshotStore snapshot) {
        return new EntitySyncTask(
                TestMappings.clanOnly(),
                mock(JdbcConnectionSource.class),
                snapshot,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                new SyncEventPublisher((topic, key, value, callback) -> {
                    throw new AssertionError("sender not exercised by the walk");
                }),
                entity -> "test.gs.sync.clans",
                // publishFlushSeconds=1 keeps the deadline wait for the
                // never-completing future short.
                new EngineConfig(60, 500_000, 5, 1));
    }
}
