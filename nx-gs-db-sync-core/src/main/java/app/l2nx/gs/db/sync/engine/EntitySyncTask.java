package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.phase.ChangeSet;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.Window;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.*;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates one cycle for one entity:
 *
 * <ol>
 *     <li>Borrow read-only connection from {@link JdbcConnectionSource}.</li>
 *     <li>{@link WindowPlanner#plan WindowPlanner} → list of windows
 *         (envelope of DB and snapshot ranges, see cdc-engine R2).</li>
 *     <li>For each window:
 *         <ul>
 *             <li>Phase 1 — primary: {@code SELECT pk, CRC32(...)}.</li>
 *             <li>Phase 1 — each child: {@code SELECT fk, BIT_XOR(CRC32(...))
 *                 GROUP BY fk}; XOR-fold into the primary hash for matching
 *                 PKs (orphan FKs dropped silently).</li>
 *             <li>Diff aggregated scan against the snapshot for the window's
 *                 PK range.</li>
 *             <li>Phase 2 — primary + each child: chunked
 *                 {@code SELECT * WHERE pk/fk IN (...)} for created ∪
 *                 updated; group child rows by FK.</li>
 *             <li>Assemble per PK via
 *                 {@link EntityMapping#mapEntity}; publish CREATED / UPDATED
 *                 events. Publish DELETED tombstones for diff.deleted PKs.
 *                 Record per-PK {@link CompletableFuture} for end-of-cycle
 *                 walk.</li>
 *         </ul>
 *     </li>
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
 *     <li>Generic {@link SQLException} mid-window (Phase 1 primary, Phase 1
 *         child, Phase 2 primary, or Phase 2 child) → abort cycle, entity
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

        long createdCount = 0L;
        long updatedCount = 0L;
        long deletedCount = 0L;
        boolean cycleAborted = false;
        boolean degradedFromTimeout = false;

        try {
            conn.setReadOnly(true);

            List<Window> windows = planner.plan(mapping, conn, snapshot,
                    config.rowsPerWindow(), config.queryTimeoutSeconds());

            for (Window window : windows) {
                Long2IntMap currentScan;
                try {
                    currentScan = hasher.hashPrimary(
                            window, mapping.primary(), conn, config.queryTimeoutSeconds());
                    foldChildrenInto(currentScan, window, conn);
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
                Map<Long, Object> assembled;
                if (createUpdate.isEmpty()) {
                    assembled = Collections.emptyMap();
                } else {
                    try {
                        assembled = assembleEntities(createUpdate, conn);
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

                // Per-window publish + flush. Bounding inFlight / pending* to a
                // single window's PK count caps cycle-resident memory at
                // O(rowsPerWindow) instead of O(totalRows). At 6.5M items with
                // rowsPerWindow=500_000 this is ~40 MB peak vs ~500 MB if the
                // accumulators were carried across all 13 windows of the cycle.
                int windowSize = currentScan.size() + diff.deleted().size();
                int presize = Math.max(16, windowSize * 2);
                Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                        new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>(presize);
                Long2IntMap pendingCrcAdvance = new Long2IntOpenHashMap(presize);
                LongSet pendingCreates = new LongOpenHashSet(presize);
                LongSet pendingDeletes = new LongOpenHashSet(diff.deleted().size() * 2);

                publishChanges(diff, assembled, currentScan, topic,
                        inFlight, pendingCrcAdvance, pendingCreates, pendingDeletes);

                long[] applied = walkInFlightAndAdvance(entity, inFlight, pendingCrcAdvance,
                        pendingCreates, pendingDeletes);
                createdCount += applied[0];
                updatedCount += applied[1];
                deletedCount += applied[2];
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
            long abortedElapsedMs = elapsed(started);
            log.info("Entity {} cycle DEGRADED (aborted), elapsedMs={}", entity, abortedElapsedMs);
            return CycleResult.degraded(abortedElapsedMs);
        }

        long rowCount = snapshot.sizeOf(entity);
        EntityState finalState = degradedFromTimeout ? EntityState.DEGRADED : EntityState.HEALTHY;
        long elapsedMs = elapsed(started);
        log.info("Entity {} cycle {}: +{} ~{} -{}, rowCount={}, elapsedMs={}",
                entity, finalState, createdCount, updatedCount, deletedCount, rowCount, elapsedMs);
        return new CycleResult(finalState, elapsedMs,
                createdCount, updatedCount, deletedCount, rowCount);
    }

    /**
     * Phase-1 children pass: for every declared {@link ChildSource}, run the
     * window-bounded {@code BIT_XOR(CRC32(...))} aggregate and XOR-fold each
     * (parent PK → aggregate CRC) into the primary scan. PKs present in a
     * child but missing from the primary scan are orphan FKs and are dropped
     * silently — the entity does not exist without a primary row.
     */
    private void foldChildrenInto(Long2IntMap currentScan,
                                  Window window,
                                  Connection conn) throws SQLException {
        for (ChildSource<?> child : mapping.children()) {
            Long2IntMap childHash = hasher.hashChild(
                    window, child, conn, config.queryTimeoutSeconds());
            for (Long2IntMap.Entry e : childHash.long2IntEntrySet()) {
                long pk = e.getLongKey();
                int xorCrc = e.getIntValue();
                if (currentScan.containsKey(pk)) {
                    currentScan.put(pk, currentScan.get(pk) ^ xorCrc);
                }
                // else: orphan child — primary row absent, drop silently
            }
        }
    }

    /**
     * Phase-2 fetch and assembly. Runs one IN-query for the primary source and
     * one per child source, groups child rows by FK, then calls
     * {@link EntityMapping#mapEntity} per PK to produce typed entity DTOs.
     *
     * <p>If a PK present in {@code createUpdate} returns no primary row (deleted
     * between Phase 1 and Phase 2), the engine omits it from the assembled
     * map; the caller's per-PK loop short-circuits via a null lookup and the
     * next cycle's Phase-1 diff catches the deletion as a tombstone.</p>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<Long, Object> assembleEntities(LongList createUpdate, Connection conn) throws SQLException {
        Long2ObjectMap<Object> primaryRows = fetcher.fetchPrimary(
                mapping.primary(), createUpdate, conn, config.queryTimeoutSeconds());

        // Per-child fetch; preserve declared order for stable mapEntity input.
        Map<String, Long2ObjectMap<List<Object>>> childRowsByTable =
                new LinkedHashMap<String, Long2ObjectMap<List<Object>>>();
        for (ChildSource<?> child : mapping.children()) {
            childRowsByTable.put(child.tableName(),
                    fetcher.fetchChild(child, createUpdate, conn, config.queryTimeoutSeconds()));
        }

        Map<Long, Object> assembled = new HashMap<Long, Object>(createUpdate.size() * 2);
        EntityMapping erased = mapping;
        LongIterator pkIt = createUpdate.iterator();
        while (pkIt.hasNext()) {
            long pk = pkIt.nextLong();
            Object primaryRow = primaryRows.get(pk);
            if (primaryRow == null) {
                // Phase-2 missing primary row — silent no-op; next cycle catches as DELETE.
                continue;
            }
            Map<String, List<Object>> children = collectChildren(pk, childRowsByTable);
            Object dto = erased.mapEntity(primaryRow, children);
            assembled.put(pk, dto);
        }
        return assembled;
    }

    private static Map<String, List<Object>> collectChildren(
            long pk, Map<String, Long2ObjectMap<List<Object>>> childRowsByTable) {
        if (childRowsByTable.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<Object>> out = new LinkedHashMap<String, List<Object>>(childRowsByTable.size() * 2);
        for (Map.Entry<String, Long2ObjectMap<List<Object>>> e : childRowsByTable.entrySet()) {
            List<Object> rows = e.getValue().get(pk);
            out.put(e.getKey(), rows == null ? Collections.emptyList() : rows);
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void publishChanges(ChangeSet diff,
                                Map<Long, Object> assembled,
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
            Object dto = assembled.get(pk);
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
            Object dto = assembled.get(pk);
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
