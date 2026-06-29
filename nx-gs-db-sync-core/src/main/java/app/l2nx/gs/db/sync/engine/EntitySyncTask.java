package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.phase.ChangeSet;
import app.l2nx.gs.db.sync.engine.phase.ConsistentSnapshotTxn;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.Window;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import it.unimi.dsi.fastutil.longs.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;

/**
 * Runs one CDC cycle for one entity: plan windows, Phase-1 CRC (primary +
 * children inside ONE consistent-snapshot txn per window), diff against
 * snapshot, Phase-2 fetch + assemble for changed PKs, publish, walk per-PK
 * futures and advance snapshot only for ack'd PKs.
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

    private final StatementRegistry statementRegistry = new StatementRegistry();
    private volatile JdbcDialect dialect;

    public EntitySyncTask(
            EntityMapping<?> mapping,
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
        return runCycle(null);
    }

    /**
     * Runs one cycle. When {@code targetedPks} is non-null and non-empty the
     * cycle takes the force-resync fast-path: Phase-1 plans targeted
     * {@code IN}-list windows over only those PKs (primary scoped by
     * {@code pk IN (...)}, children by {@code fk IN (...)}) instead of a
     * full-table range scan. The diff / Phase-2 / publish / snapshot-advance
     * logic is identical — ghost PKs (in {@code targetedPks} but with no live
     * row) still fall out as DELETE because the IN-list returns no row and the
     * perturbed snapshot entry has no scan match. A null/empty set is the
     * full-scan path (scheduled tick / whole-entity resync).
     */
    public CycleResult runCycle(@Nullable LongSet targetedPks) {
        long started = System.currentTimeMillis();
        String entity = mapping.entityName();
        boolean targeted = targetedPks != null && !targetedPks.isEmpty();

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
            log.error(
                    "Entity {} JdbcConnectionSource.getConnection threw {}: {}",
                    entity,
                    borrowBug.getClass().getName(),
                    borrowBug.getMessage());
            return CycleResult.degraded(elapsed(started));
        }

        long createdCount = 0L;
        long updatedCount = 0L;
        long deletedCount = 0L;
        long failedPublishCount = 0L;
        long pendingPublishCount = 0L;
        boolean cycleAborted = false;
        boolean degradedFromTimeout = false;

        try {
            conn.setReadOnly(true);
            if (dialect == null) {
                JdbcDialect detected = JdbcDialect.detect(conn);
                dialect = detected;
                log.info("Entity {} JDBC dialect detected: {}", entity, detected);
            }

            List<Window> windows;
            Long2ObjectOpenHashMap<LongSet> snapshotBuckets;
            if (targeted) {
                windows = planner.planTargeted(mapping, conn, targetedPks);
                snapshotBuckets = snapshot.bucketByTargetedWindows(entity, windows);
            } else {
                windows = planner.plan(mapping, conn, snapshot, config.rowsPerWindow(), config.queryTimeoutSeconds());
                snapshotBuckets = snapshot.bucketByWindows(entity, windows);
            }

            for (int wIdx = 0; wIdx < windows.size(); wIdx++) {
                Window window = windows.get(wIdx);
                Long2IntMap currentScan;
                try {
                    final Window finalWindow = window;
                    currentScan = ConsistentSnapshotTxn.runReadOnly(conn, txnConn -> {
                        Long2IntMap primaryScan = hasher.hashPrimary(
                                txnConn,
                                finalWindow,
                                mapping.primary(),
                                config.queryTimeoutSeconds(),
                                config.fetchSize(),
                                dialect,
                                statementRegistry);
                        foldChildrenInto(primaryScan, finalWindow, txnConn);
                        return primaryScan;
                    });
                } catch (SQLTimeoutException timeout) {
                    log.warn("Entity {} window {} Phase-1 timed out — DEGRADED, skipping window", entity, window);
                    degradedFromTimeout = true;
                    continue;
                } catch (SQLException sqlError) {
                    log.error(
                            "Entity {} window {} Phase-1 SQL error: {} — aborting cycle",
                            entity,
                            window,
                            sqlError.getMessage());
                    cycleAborted = true;
                    break;
                }

                LongSet prevKeys = snapshotBuckets.get(wIdx);
                if (prevKeys == null) {
                    prevKeys = new LongOpenHashSet();
                }
                ChangeSet diff = ChangeSet.diff(currentScan, prevKeys, snapshot, entity);

                LongList createUpdate = unionToList(diff.created(), diff.updated());
                Long2ObjectMap<Object> assembled;
                if (createUpdate.isEmpty()) {
                    assembled = Long2ObjectMaps.emptyMap();
                } else {
                    try {
                        // Phase-2 fetches fresh rows post-Phase-1; rows can move between
                        // the two — engine treats missing-in-Phase-2 as silent no-op and
                        // re-detects via next cycle's Phase-1 diff.
                        assembled = assembleEntities(createUpdate, conn);
                    } catch (SQLTimeoutException timeout) {
                        log.warn("Entity {} window {} Phase-2 timed out — DEGRADED, skipping window", entity, window);
                        degradedFromTimeout = true;
                        continue;
                    } catch (SQLException sqlError) {
                        log.error(
                                "Entity {} window {} Phase-2 SQL error: {} — aborting cycle",
                                entity,
                                window,
                                sqlError.getMessage());
                        cycleAborted = true;
                        break;
                    }
                }

                int createdSize = diff.created().size();
                int updatedSize = diff.updated().size();
                int deletedSize = diff.deleted().size();
                int crcAdvanceSize = createdSize + updatedSize;
                int totalChanges = crcAdvanceSize + deletedSize;
                Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight =
                        new Long2ObjectOpenHashMap<CompletableFuture<RecordMetadata>>(totalChanges);
                Long2IntMap pendingCrcAdvance = new Long2IntOpenHashMap(crcAdvanceSize);
                ((Long2IntOpenHashMap) pendingCrcAdvance).defaultReturnValue(Phase1Hasher.MISSING_HASH);
                LongSet pendingCreates = new LongOpenHashSet(createdSize);
                LongSet pendingDeletes = new LongOpenHashSet(deletedSize);

                publishChanges(
                        diff,
                        assembled,
                        currentScan,
                        topic,
                        inFlight,
                        pendingCrcAdvance,
                        pendingCreates,
                        pendingDeletes);

                long[] applied =
                        walkInFlightAndAdvance(entity, inFlight, pendingCrcAdvance, pendingCreates, pendingDeletes);
                createdCount += applied[0];
                updatedCount += applied[1];
                deletedCount += applied[2];
                failedPublishCount += applied[3];
                pendingPublishCount += applied[4];
            }
        } catch (SQLException unexpectedSqlError) {
            log.error("Entity {} cycle SQL error: {} — aborting", entity, unexpectedSqlError.getMessage());
            cycleAborted = true;
        } finally {
            try {
                conn.close();
            } catch (Throwable closeError) {
                log.warn("Entity {} connection close failed: {}", entity, closeError);
            }
            statementRegistry.clear();
        }

        if (cycleAborted) {
            long abortedElapsedMs = elapsed(started);
            log.info("Entity {} cycle DEGRADED (aborted), elapsedMs={}", entity, abortedElapsedMs);
            return CycleResult.degraded(abortedElapsedMs);
        }

        long rowCount = snapshot.sizeOf(entity);
        EntityState finalState = degradedFromTimeout ? EntityState.DEGRADED : EntityState.HEALTHY;
        long elapsedMs = elapsed(started);
        log.debug(
                "Entity {} cycle {}: +{} ~{} -{}, rowCount={}, elapsedMs={}",
                entity,
                finalState,
                createdCount,
                updatedCount,
                deletedCount,
                rowCount,
                elapsedMs);
        return new CycleResult(
                finalState,
                elapsedMs,
                createdCount,
                updatedCount,
                deletedCount,
                rowCount,
                failedPublishCount,
                pendingPublishCount);
    }

    /**
     * Cancel any in-flight JDBC statement on this task. Called by the engine
     * on stop so a hung query inside Phase-1 / Phase-2 is interrupted instead
     * of pinning the daemon thread until the driver-side socket timeout fires.
     */
    public void cancelCurrentStatement() {
        statementRegistry.cancelCurrent();
    }

    private void foldChildrenInto(Long2IntMap currentScan, Window window, Connection conn) throws SQLException {
        for (ChildSource<?> child : mapping.children()) {
            Long2IntMap childHash = hasher.hashChild(
                    conn, window, child, config.queryTimeoutSeconds(), config.fetchSize(), dialect, statementRegistry);
            for (Long2IntMap.Entry e : childHash.long2IntEntrySet()) {
                long pk = e.getLongKey();
                int xorCrc = e.getIntValue();
                if (currentScan.containsKey(pk)) {
                    currentScan.put(pk, currentScan.get(pk) ^ xorCrc);
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Long2ObjectMap<Object> assembleEntities(LongList createUpdate, Connection conn) throws SQLException {
        Long2ObjectMap<Object> primaryRows = fetcher.fetchPrimary(
                conn,
                mapping.primary(),
                createUpdate,
                config.queryTimeoutSeconds(),
                config.fetchSize(),
                dialect,
                statementRegistry);

        Map<String, Long2ObjectMap<List<Object>>> childRowsByTable =
                new LinkedHashMap<String, Long2ObjectMap<List<Object>>>();
        for (ChildSource<?> child : mapping.children()) {
            childRowsByTable.put(
                    child.tableName(),
                    fetcher.fetchChild(
                            conn,
                            child,
                            createUpdate,
                            config.queryTimeoutSeconds(),
                            config.fetchSize(),
                            dialect,
                            statementRegistry));
        }

        Long2ObjectMap<Object> assembled = new Long2ObjectOpenHashMap<Object>(createUpdate.size());
        EntityMapping erased = mapping;
        LongIterator pkIt = createUpdate.iterator();
        while (pkIt.hasNext()) {
            long pk = pkIt.nextLong();
            Object primaryRow = primaryRows.get(pk);
            if (primaryRow == null) {
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
    private void publishChanges(
            ChangeSet diff,
            Long2ObjectMap<Object> assembled,
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
            CompletableFuture<RecordMetadata> f =
                    publisher.publish(erased, SyncEventPublisher.OP_CREATED, pk, dto, topic);
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
            CompletableFuture<RecordMetadata> f =
                    publisher.publish(erased, SyncEventPublisher.OP_UPDATED, pk, dto, topic);
            inFlight.put(pk, f);
            pendingCrcAdvance.put(pk, currentScan.get(pk));
        }
        LongIterator deleteIt = diff.deleted().iterator();
        while (deleteIt.hasNext()) {
            long pk = deleteIt.nextLong();
            CompletableFuture<RecordMetadata> f =
                    publisher.publish(erased, SyncEventPublisher.OP_DELETED, pk, null, topic);
            inFlight.put(pk, f);
            pendingDeletes.add(pk);
        }
    }

    /**
     * Walk per-PK publish futures. Already-completed futures are drained first
     * (cheap, no get-with-timeout) so a slow publish at the head of iteration
     * order can't starve already-acked publishes that follow. Remaining
     * pending futures share a single deadline; whatever's still pending past
     * the deadline replays next cycle.
     *
     * <p>Returns {@code [created, updated, deleted, failed, pending]} —
     * indexes 3/4 count publishes that failed exceptionally / outlived the
     * flush deadline; both feed the cycle's fully-successful gate.
     * Package-private for the publish-walk unit tests.</p>
     */
    long[] walkInFlightAndAdvance(
            String entity,
            Long2ObjectMap<CompletableFuture<RecordMetadata>> inFlight,
            Long2IntMap pendingCrcAdvance,
            LongSet pendingCreates,
            LongSet pendingDeletes) {
        long[] tally = new long[5];
        long failed = 0L;
        long pending = 0L;

        LongList stillPending = new LongArrayList();
        LongList doneSuccess = new LongArrayList();

        for (Long2ObjectMap.Entry<CompletableFuture<RecordMetadata>> e : inFlight.long2ObjectEntrySet()) {
            long pk = e.getLongKey();
            CompletableFuture<RecordMetadata> future = e.getValue();

            if (future.isDone()) {
                if (future.isCompletedExceptionally()) {
                    if (failed == 0L) {
                        Throwable cause = extractCause(future);
                        log.warn(
                                "Entity {} first publish failure: {} — replaying failed PKs next cycle",
                                entity,
                                cause == null ? "<unknown>" : cause.getMessage());
                    }
                    failed++;
                } else {
                    doneSuccess.add(pk);
                }
            } else {
                stillPending.add(pk);
            }
        }

        for (LongIterator it = doneSuccess.iterator(); it.hasNext(); ) {
            applyAck(entity, it.nextLong(), pendingCrcAdvance, pendingCreates, pendingDeletes, tally);
        }

        if (!stillPending.isEmpty()) {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.publishFlushSeconds());
            for (LongIterator it = stillPending.iterator(); it.hasNext(); ) {
                long pk = it.nextLong();
                CompletableFuture<RecordMetadata> future = inFlight.get(pk);
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos < 0L) {
                    remainingNanos = 0L;
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
                        log.warn(
                                "Entity {} first publish failure: {} — replaying failed PKs next cycle",
                                entity,
                                rootCause(ex).getMessage());
                    }
                    failed++;
                    continue;
                }
                applyAck(entity, pk, pendingCrcAdvance, pendingCreates, pendingDeletes, tally);
            }
        }

        if (failed > 1L) {
            log.warn("Entity {} {} additional publish failures suppressed", entity, failed - 1L);
        }
        if (pending > 0L) {
            log.warn(
                    "Entity {} {} publishes still pending past flush deadline ({}s) — replaying next cycle",
                    entity,
                    pending,
                    config.publishFlushSeconds());
        }
        tally[3] = failed;
        tally[4] = pending;
        return tally;
    }

    private void applyAck(
            String entity,
            long pk,
            Long2IntMap pendingCrcAdvance,
            LongSet pendingCreates,
            LongSet pendingDeletes,
            long[] tally) {
        if (pendingDeletes.contains(pk)) {
            snapshot.removeCrc(entity, pk);
            tally[2]++;
        } else if (pendingCreates.contains(pk)) {
            snapshot.putCrc(entity, pk, pendingCrcAdvance.get(pk));
            tally[0]++;
        } else {
            snapshot.putCrc(entity, pk, pendingCrcAdvance.get(pk));
            tally[1]++;
        }
    }

    private static @Nullable Throwable extractCause(CompletableFuture<?> future) {
        try {
            future.getNow(null);
        } catch (CompletionException ce) {
            return ce.getCause() != null ? ce.getCause() : ce;
        } catch (CancellationException ce) {
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
