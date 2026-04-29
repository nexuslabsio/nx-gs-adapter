package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.phase.ChangeSet;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.Window;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.*;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates one cycle for one entity:
 *
 * <ol>
 *     <li>Borrow read-only connection from {@link JdbcConnectionSource}.</li>
 *     <li>{@link WindowPlanner#plan WindowPlanner} → list of windows.</li>
 *     <li>For each window: Phase-1 hash → diff against snapshot → Phase-2
 *         fetch (created ∪ updated) → publish per PK, recording per-PK
 *         {@link CompletableFuture} for end-of-cycle walk.</li>
 *     <li>End-of-cycle: walk per-PK futures up to {@code publishFlushSeconds};
 *         advance {@link SnapshotStore} only for PKs whose publish actually
 *         succeeded. Failed / timed-out PKs stay in the previous snapshot and
 *         get replayed next cycle.</li>
 * </ol>
 *
 * <p>DEGRADED triage:</p>
 * <ul>
 *     <li>Borrow failure or generic borrow {@link SQLException} → entity
 *         DEGRADED, snapshot untouched, no Kafka publishes.</li>
 *     <li>Missing topic for the entity → entity DEGRADED every cycle, no
 *         publishes, no diff.</li>
 *     <li>{@link SQLTimeoutException} mid-window → window skipped, entity
 *         DEGRADED for the cycle, continue to next window.</li>
 *     <li>Generic {@link SQLException} mid-window → abort cycle, entity
 *         DEGRADED, snapshot frozen, next tick retries from the top.</li>
 * </ul>
 */
public final class EntitySyncTask {

    private static final NxLog log = NxLogFactory.getLogger(EntitySyncTask.class);

    private final EntityMapping<?> mapping;
    private final JdbcConnectionSource jdbcSource;
    private final SnapshotStore snapshot;
    private final WindowPlanner planner;
    private final Phase1Hasher hasher;
    private final Phase2Fetcher fetcher;
    private final SyncEventPublisher publisher;
    private final TopicResolver topicResolver;
    private final EngineConfig config;

    public EntitySyncTask(EntityMapping<?> mapping,
                          JdbcConnectionSource jdbcSource,
                          SnapshotStore snapshot,
                          WindowPlanner planner,
                          Phase1Hasher hasher,
                          Phase2Fetcher fetcher,
                          SyncEventPublisher publisher,
                          TopicResolver topicResolver,
                          EngineConfig config) {
        this.mapping = mapping;
        this.jdbcSource = jdbcSource;
        this.snapshot = snapshot;
        this.planner = planner;
        this.hasher = hasher;
        this.fetcher = fetcher;
        this.publisher = publisher;
        this.topicResolver = topicResolver;
        this.config = config;
    }

    public CycleResult runCycle() {
        long started = System.currentTimeMillis();
        String entity = mapping.entityName();

        String topic = topicResolver.resolveTopic(entity);
        if (topic == null) {
            log.warn("Entity {} has no topic in syncTopics — DEGRADED, skipping cycle", entity);
            return CycleResult.degraded(elapsed(started));
        }

        Connection conn;
        try {
            conn = jdbcSource.getConnection();
        } catch (SQLException borrowFailure) {
            log.error("Entity {} borrow failed: {}", entity, borrowFailure.getMessage());
            return CycleResult.degraded(elapsed(started));
        } catch (RuntimeException borrowBug) {
            log.error("Entity {} JdbcConnectionSource.getConnection threw {}: {}",
                    entity, borrowBug.getClass().getName(), borrowBug.getMessage());
            return CycleResult.degraded(elapsed(started));
        }

        Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>();
        Long2IntMap pendingCrcAdvance = new Long2IntOpenHashMap();
        LongSet pendingCreates = new LongOpenHashSet();
        LongSet pendingDeletes = new LongOpenHashSet();
        long createdCount = 0L;
        long updatedCount = 0L;
        long deletedCount = 0L;
        boolean cycleAborted = false;
        boolean degradedFromTimeout = false;

        try {
            conn.setReadOnly(true);

            List<Window> windows = planner.plan(mapping, conn,
                    config.rowsPerWindow(), config.queryTimeoutSeconds());

            for (Window window : windows) {
                Long2IntMap currentScan;
                try {
                    currentScan = hasher.hash(window, mapping, conn, config.queryTimeoutSeconds());
                } catch (SQLTimeoutException timeout) {
                    log.warn("Entity {} window {} Phase-1 timed out — DEGRADED, skipping window",
                            entity, window);
                    degradedFromTimeout = true;
                    continue;
                } catch (SQLException sqlError) {
                    log.error("Entity {} window {} Phase-1 SQL error: {} — aborting cycle",
                            entity, window, sqlError.getMessage());
                    cycleAborted = true;
                    break;
                }

                LongSet prevKeys = snapshot.keysInRange(entity, window.fromPk(), window.toPk());
                ChangeSet diff = ChangeSet.diff(currentScan, prevKeys, snapshot, entity);

                LongList createUpdate = unionToList(diff.created(), diff.updated());
                Long2ObjectMap<?> rows;
                if (createUpdate.isEmpty()) {
                    rows = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<Object>();
                } else {
                    try {
                        rows = fetchRows(mapping, createUpdate, conn);
                    } catch (SQLTimeoutException timeout) {
                        log.warn("Entity {} window {} Phase-2 timed out — DEGRADED, skipping window",
                                entity, window);
                        degradedFromTimeout = true;
                        continue;
                    } catch (SQLException sqlError) {
                        log.error("Entity {} window {} Phase-2 SQL error: {} — aborting cycle",
                                entity, window, sqlError.getMessage());
                        cycleAborted = true;
                        break;
                    }
                }

                publishChanges(diff, rows, currentScan, topic,
                        inFlight, pendingCrcAdvance, pendingCreates, pendingDeletes);
            }
        } catch (SQLException unexpectedSqlError) {
            log.error("Entity {} cycle SQL error: {} — aborting", entity, unexpectedSqlError.getMessage());
            cycleAborted = true;
        } finally {
            try {
                conn.close();
            } catch (SQLException closeError) {
                log.warn("Entity {} connection close failed: {}", entity, closeError.getMessage());
            }
        }

        if (cycleAborted) {
            return CycleResult.degraded(elapsed(started));
        }

        long[] applied = walkInFlightAndAdvance(entity, inFlight, pendingCrcAdvance,
                pendingCreates, pendingDeletes);
        createdCount = applied[0];
        updatedCount = applied[1];
        deletedCount = applied[2];

        long rowCount = snapshot.sizeOf(entity);
        EntityState finalState = degradedFromTimeout ? EntityState.DEGRADED : EntityState.HEALTHY;
        return new CycleResult(finalState, elapsed(started),
                createdCount, updatedCount, deletedCount, rowCount);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Long2ObjectMap<?> fetchRows(EntityMapping<?> mapping, LongList pks, Connection conn) throws SQLException {
        EntityMapping erased = mapping;
        return fetcher.fetch(erased, pks, conn, config.queryTimeoutSeconds());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void publishChanges(ChangeSet diff,
                                Long2ObjectMap<?> rows,
                                Long2IntMap currentScan,
                                String topic,
                                Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight,
                                Long2IntMap pendingCrcAdvance,
                                LongSet pendingCreates,
                                LongSet pendingDeletes) {
        EntityMapping erased = mapping;
        LongIterator createIt = diff.created().iterator();
        while (createIt.hasNext()) {
            long pk = createIt.nextLong();
            Object dto = rows.get(pk);
            if (dto == null) {
                continue;
            }
            CompletableFuture<RecordMetadata> f = publisher.publish(
                    erased, SyncEventPublisher.OP_CREATED, pk, dto, topic);
            inFlight.put(pk, f);
            pendingCrcAdvance.put(pk, currentScan.get(pk));
            pendingCreates.add(pk);
        }
        LongIterator updateIt = diff.updated().iterator();
        while (updateIt.hasNext()) {
            long pk = updateIt.nextLong();
            Object dto = rows.get(pk);
            if (dto == null) {
                continue;
            }
            CompletableFuture<RecordMetadata> f = publisher.publish(
                    erased, SyncEventPublisher.OP_UPDATED, pk, dto, topic);
            inFlight.put(pk, f);
            pendingCrcAdvance.put(pk, currentScan.get(pk));
        }
        LongIterator deleteIt = diff.deleted().iterator();
        while (deleteIt.hasNext()) {
            long pk = deleteIt.nextLong();
            CompletableFuture<RecordMetadata> f = publisher.publish(
                    erased, SyncEventPublisher.OP_DELETED, pk, null, topic);
            inFlight.put(pk, f);
            pendingDeletes.add(pk);
        }
    }

    /**
     * Walk per-PK publish futures with a single shared {@code publishFlushSeconds}
     * deadline. Already-completed futures are drained first (cheap), so a slow
     * publish at the head of the iteration order can't starve all already-acked
     * publishes that follow. Pending-but-undone futures past the deadline are
     * counted and reported as a single summary line — they'll replay next cycle.
     */
    private long[] walkInFlightAndAdvance(String entity,
                                          Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight,
                                          Long2IntMap pendingCrcAdvance,
                                          LongSet pendingCreates,
                                          LongSet pendingDeletes) {
        long created = 0L;
        long updated = 0L;
        long deleted = 0L;
        long failed = 0L;
        long pending = 0L;

        long deadlineNanos = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(config.publishFlushSeconds());

        for (Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>> e
                : inFlight.long2ObjectEntrySet()) {
            long pk = e.getLongKey();
            CompletableFuture<RecordMetadata> future = e.getValue();

            if (!future.isDone()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    pending++;
                    continue;
                }
                try {
                    future.get(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    pending++;
                    continue;
                } catch (TimeoutException te) {
                    pending++;
                    continue;
                } catch (ExecutionException ex) {
                    if (failed == 0L) {
                        log.warn("Entity {} first publish failure: {} — replaying failed PKs next cycle",
                                entity, rootCause(ex).getMessage());
                    }
                    failed++;
                    continue;
                }
            } else if (future.isCompletedExceptionally()) {
                if (failed == 0L) {
                    Throwable cause = extractCause(future);
                    log.warn("Entity {} first publish failure: {} — replaying failed PKs next cycle",
                            entity, cause == null ? "<unknown>" : cause.getMessage());
                }
                failed++;
                continue;
            }

            if (pendingDeletes.contains(pk)) {
                snapshot.removeCrc(entity, pk);
                deleted++;
            } else if (pendingCreates.contains(pk)) {
                snapshot.putCrc(entity, pk, pendingCrcAdvance.get(pk));
                created++;
            } else {
                snapshot.putCrc(entity, pk, pendingCrcAdvance.get(pk));
                updated++;
            }
        }
        if (failed > 1L) {
            log.warn("Entity {} {} additional publish failures suppressed", entity, failed - 1L);
        }
        if (pending > 0L) {
            log.warn("Entity {} {} publishes still pending past flush deadline ({}s) — replaying next cycle",
                    entity, pending, config.publishFlushSeconds());
        }
        return new long[]{created, updated, deleted};
    }

    private static Throwable extractCause(CompletableFuture<?> future) {
        try {
            future.getNow(null);
        } catch (java.util.concurrent.CompletionException ce) {
            return ce.getCause() != null ? ce.getCause() : ce;
        } catch (java.util.concurrent.CancellationException ce) {
            return ce;
        }
        return null;
    }

    private static LongList unionToList(LongSet a, LongSet b) {
        LongArrayList list = new LongArrayList(a.size() + b.size());
        LongIterator ai = a.iterator();
        while (ai.hasNext()) {
            list.add(ai.nextLong());
        }
        LongIterator bi = b.iterator();
        while (bi.hasNext()) {
            list.add(bi.nextLong());
        }
        return list;
    }

    private static long elapsed(long startedMs) {
        return System.currentTimeMillis() - startedMs;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t.getCause();
        return cause != null ? cause : t;
    }
}
