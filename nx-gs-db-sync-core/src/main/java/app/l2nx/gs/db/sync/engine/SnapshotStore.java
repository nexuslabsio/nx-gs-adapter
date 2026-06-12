package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory primitive-keyed CRC32 snapshot, one {@link Long2IntOpenHashMap} per
 * synced entity. Open-hash backing keeps per-entry footprint ~16 bytes — at
 * 6.5M+ items a tree-backed map would burn ~360 MB. Range / extreme lookups
 * become O(N) scans but happen on a dedicated daemon thread per entity.
 *
 * <p>Sentinel: {@code defaultReturnValue} is set to
 * {@link Phase1Hasher#MISSING_HASH} so callers can compare lookup-result
 * directly without a separate {@code containsKey} round-trip when 0 is a
 * legitimate CRC32 value.</p>
 *
 * <p>Thread safety: the outer per-entity registries are concurrent — different
 * entities' cycles run on different CDC pool threads, and the first
 * {@code putCrc} / {@code invalidate} for an entity structurally modifies the
 * outer map on that entity's thread while other entities' threads do the same.
 * Each inner {@code Long2IntOpenHashMap} stays single-writer: every mutation
 * for a given entity happens on that entity's cycle thread (under the engine's
 * per-entity ticking guard), so the inner maps need no synchronization.</p>
 */
public final class SnapshotStore {

    private final Map<String, Long2IntOpenHashMap> byEntity =
            new ConcurrentHashMap<String, Long2IntOpenHashMap>();
    private final Map<String, ExtremeCache> extremeCache =
            new ConcurrentHashMap<String, ExtremeCache>();

    public int getCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null) {
            return Phase1Hasher.MISSING_HASH;
        }
        return map.get(pk);
    }

    public boolean containsCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        return map != null && map.containsKey(pk);
    }

    public void putCrc(String entityName, long pk, int crc) {
        Long2IntOpenHashMap map = mapOf(entityName);
        int prior = map.put(pk, crc);
        ExtremeCache cache = extremeCache.get(entityName);
        if (cache != null && prior == Phase1Hasher.MISSING_HASH) {
            cache.observePut(pk);
        }
    }

    public void removeCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null) {
            return;
        }
        int prior = map.remove(pk);
        if (prior == Phase1Hasher.MISSING_HASH) {
            return;
        }
        ExtremeCache cache = extremeCache.get(entityName);
        if (cache != null) {
            cache.observeRemove(pk);
        }
    }

    /**
     * Force-resync invalidation of one PK. MUST be called from the entity's
     * cycle thread only (same single-writer contract as every other mutation).
     *
     * <p>A PK present in the snapshot gets its stored CRC perturbed to a value
     * guaranteed to differ from the stored one and never equal to
     * {@link Phase1Hasher#MISSING_HASH}, so the next cycle's diff classifies
     * the row as UPDATED (live row) or DELETED (row gone from the host DB). A
     * PK absent from the snapshot gets a sentinel entry inserted so the diff
     * emits DELETED when the host DB has no such row — repairing platform-side
     * ghost rows the snapshot never knew.</p>
     */
    public void invalidate(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || !map.containsKey(pk)) {
            putCrc(entityName, pk, INVALIDATION_SENTINEL);
            return;
        }
        putCrc(entityName, pk, perturb(map.get(pk)));
    }

    /**
     * Force-resync invalidation of every stored PK of one entity. MUST be
     * called from the entity's cycle thread only. Perturbs every stored value
     * in place (zero-alloc fastIterator walk); min/max extremes are unaffected
     * because only values change.
     */
    public void invalidateAll(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return;
        }
        ObjectIterator<Long2IntMap.Entry> it = map.long2IntEntrySet().fastIterator();
        while (it.hasNext()) {
            Long2IntMap.Entry e = it.next();
            e.setValue(perturb(e.getIntValue()));
        }
    }

    /**
     * Stored value for an invalidated PK the snapshot never had. Any value
     * works as long as it differs from {@link Phase1Hasher#MISSING_HASH} (the
     * map's defaultReturnValue the put/remove bookkeeping keys on) — a live
     * host row hashes to the real CRC and mismatches with probability
     * 1 - 2^-32, an absent row diffs to DELETED regardless of the value.
     */
    static final int INVALIDATION_SENTINEL = Phase1Hasher.MISSING_HASH ^ 1;

    private static int perturb(int crc) {
        int flipped = crc ^ 1;
        return flipped == Phase1Hasher.MISSING_HASH ? crc ^ 2 : flipped;
    }

    /**
     * Returns the PKs in {@code [fromPk, toPk]} (closed interval) currently
     * stored for the entity. O(N) full-keys scan on first call per cycle;
     * subsequent windows in the same cycle should consult
     * {@link #bucketByWindows} for amortized O(1) per window.
     */
    public LongSet keysInRange(String entityName, long fromPk, long toPk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return new LongOpenHashSet();
        }
        LongOpenHashSet result = new LongOpenHashSet();
        LongIterator it = map.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            if (pk >= fromPk && pk <= toPk) {
                result.add(pk);
            }
        }
        return result;
    }

    /**
     * Single-pass bucketing of an entity's snapshot keys across an ordered
     * window list. Returned map is keyed by window index (0..windows.size()-1)
     * → LongSet of PKs falling inside that window. Windows must be ordered
     * and non-overlapping (the engine constructs them that way). PKs outside
     * every window are dropped — the planner's envelope guarantees this is
     * never a real case, but no need to assert.
     */
    public Long2ObjectOpenHashMap<LongSet> bucketByWindows(String entityName, List<Window> windows) {
        Long2ObjectOpenHashMap<LongSet> buckets = new Long2ObjectOpenHashMap<LongSet>(windows.size());
        for (int i = 0; i < windows.size(); i++) {
            buckets.put(i, new LongOpenHashSet());
        }
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty() || windows.isEmpty()) {
            return buckets;
        }
        LongIterator it = map.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            int idx = findWindow(windows, pk);
            if (idx >= 0) {
                buckets.get(idx).add(pk);
            }
        }
        return buckets;
    }

    private static int findWindow(List<Window> windows, long pk) {
        int lo = 0;
        int hi = windows.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            Window w = windows.get(mid);
            if (pk < w.fromPk()) {
                hi = mid - 1;
            } else if (pk > w.toPk()) {
                lo = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    public int sizeOf(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        return map == null ? 0 : map.size();
    }

    public OptionalLong minPk(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(cacheOf(entityName, map).min(map));
    }

    public OptionalLong maxPk(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(cacheOf(entityName, map).max(map));
    }

    /**
     * Snapshot of entity names currently tracked. Stable (copy-on-read) so callers
     * can iterate without worrying about a concurrent {@link #clearEntity} reshuffle.
     */
    public Set<String> entityNames() {
        return new LinkedHashSet<String>(byEntity.keySet());
    }

    /**
     * Streaming iteration over all (pk, crc) entries for one entity. No-op when
     * the entity is unknown. Iteration order is unspecified.
     *
     * <p>Uses fastutil's {@code fastIterator()} — the entry view is reused
     * across the loop, so a 6.5M-entry dump allocates zero {@code Entry}
     * instances instead of 6.5M.</p>
     */
    public void forEachEntry(String entityName, EntryConsumer consumer) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return;
        }
        ObjectIterator<Long2IntMap.Entry> it = map.long2IntEntrySet().fastIterator();
        while (it.hasNext()) {
            Long2IntMap.Entry e = it.next();
            consumer.accept(e.getLongKey(), e.getIntValue());
        }
    }

    /**
     * Streaming bulk-load entry point. Returns a {@link Loader} the caller
     * fills via {@link Loader#put(long, int)} and finalizes via
     * {@link Loader#commit()} once the source is fully decoded; if the source
     * fails mid-decode the loader is simply abandoned — partial state never
     * reaches the live store.
     */
    public Loader newLoader(String entityName, int sizeHint) {
        Long2IntOpenHashMap fresh = new Long2IntOpenHashMap(sizeHint);
        fresh.defaultReturnValue(Phase1Hasher.MISSING_HASH);
        return new Loader(this, entityName, fresh);
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(long pk, int crc);
    }

    public static final class Loader {
        private final SnapshotStore parent;
        private final String entityName;
        private final Long2IntOpenHashMap fresh;

        private Loader(SnapshotStore parent, String entityName, Long2IntOpenHashMap fresh) {
            this.parent = parent;
            this.entityName = entityName;
            this.fresh = fresh;
        }

        public void put(long pk, int crc) {
            fresh.put(pk, crc);
        }

        public int size() {
            return fresh.size();
        }

        public void commit() {
            parent.byEntity.put(entityName, fresh);
            parent.extremeCache.remove(entityName);
        }
    }

    public void clearEntity(String entityName) {
        byEntity.remove(entityName);
        extremeCache.remove(entityName);
    }

    public void clearAll() {
        byEntity.clear();
        extremeCache.clear();
    }

    private Long2IntOpenHashMap mapOf(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null) {
            map = new Long2IntOpenHashMap();
            map.defaultReturnValue(Phase1Hasher.MISSING_HASH);
            byEntity.put(entityName, map);
        }
        return map;
    }

    private ExtremeCache cacheOf(String entityName, Long2IntOpenHashMap map) {
        ExtremeCache cache = extremeCache.get(entityName);
        if (cache == null) {
            cache = new ExtremeCache();
            extremeCache.put(entityName, cache);
        }
        return cache;
    }

    /**
     * Lazy memoization of min/max PK. Insert tracks new extremes incrementally;
     * remove of a non-extreme PK is a no-op; remove of an extreme marks dirty
     * and the next read recomputes via one O(N) scan.
     */
    private static final class ExtremeCache {
        private boolean valid;
        private long min;
        private long max;

        void observePut(long pk) {
            if (!valid) {
                return;
            }
            if (pk < min) min = pk;
            if (pk > max) max = pk;
        }

        void observeRemove(long pk) {
            if (!valid) {
                return;
            }
            if (pk == min || pk == max) {
                valid = false;
            }
        }

        long min(Long2IntOpenHashMap map) {
            if (!valid) {
                recompute(map);
            }
            return min;
        }

        long max(Long2IntOpenHashMap map) {
            if (!valid) {
                recompute(map);
            }
            return max;
        }

        private void recompute(Long2IntOpenHashMap map) {
            long mn = Long.MAX_VALUE;
            long mx = Long.MIN_VALUE;
            LongIterator it = map.keySet().iterator();
            while (it.hasNext()) {
                long pk = it.nextLong();
                if (pk < mn) mn = pk;
                if (pk > mx) mx = pk;
            }
            min = mn;
            max = mx;
            valid = true;
        }
    }
}
