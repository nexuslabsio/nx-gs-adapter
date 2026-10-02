package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.window.Window;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-entity primitive-keyed CRC32 snapshot ({@link Long2IntOpenHashMap}: ~16 bytes/entry, a tree map would burn ~360 MB at 6.5M items);
 * range/extreme lookups are O(N) scans. {@code defaultReturnValue} is {@link Phase1Hasher#MISSING_HASH} because 0 is a valid CRC32.
 * Outer registries are concurrent; each inner map is single-writer (its entity's cycle thread under the ticking guard).
 */
public final class SnapshotStore {

    private final Map<String, Long2IntOpenHashMap> byEntity = new ConcurrentHashMap<String, Long2IntOpenHashMap>();
    private final Map<String, ExtremeCache> extremeCache = new ConcurrentHashMap<String, ExtremeCache>();

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
     * Perturbs a present PK's CRC (never equal to the stored one or MISSING_HASH) so the diff yields UPDATED/DELETED;
     * an absent PK gets a sentinel so a missing host row emits DELETED (repairs platform ghosts). Cycle thread only.
     */
    public void invalidate(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || !map.containsKey(pk)) {
            putCrc(entityName, pk, INVALIDATION_SENTINEL);
            return;
        }
        putCrc(entityName, pk, perturb(map.get(pk)));
    }

    /** Perturbs every stored CRC in place (zero-alloc); min/max unaffected. Cycle thread only. */
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

    /** Any value != MISSING_HASH works: a live row mismatches with probability 1 - 2^-32, an absent row diffs to DELETED regardless. */
    static final int INVALIDATION_SENTINEL = Phase1Hasher.MISSING_HASH ^ 1;

    private static int perturb(int crc) {
        int flipped = crc ^ 1;
        return flipped == Phase1Hasher.MISSING_HASH ? crc ^ 2 : flipped;
    }

    /** PKs in the closed interval {@code [fromPk, toPk]}; O(N) scan, use {@link #bucketByWindows} for multiple windows per cycle. */
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

    /** One-pass bucketing of snapshot keys by window index; windows must be ordered and non-overlapping, PKs outside all windows are dropped. */
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

    /** Bucketing for the targeted fast-path: per window, only its {@code IN}-list PKs present in the snapshot (drain sentinels included, so a missing live row diffs to DELETE); no O(N) scan. */
    public Long2ObjectOpenHashMap<LongSet> bucketByTargetedWindows(String entityName, List<Window> windows) {
        Long2ObjectOpenHashMap<LongSet> buckets = new Long2ObjectOpenHashMap<LongSet>(windows.size());
        Long2IntOpenHashMap map = byEntity.get(entityName);
        for (int i = 0; i < windows.size(); i++) {
            LongOpenHashSet bucket = new LongOpenHashSet();
            buckets.put(i, bucket);
            Window window = windows.get(i);
            LongList pks = window.pks();
            if (map == null || map.isEmpty() || pks == null) {
                continue;
            }
            for (int j = 0; j < pks.size(); j++) {
                long pk = pks.getLong(j);
                if (map.containsKey(pk)) {
                    bucket.add(pk);
                }
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

    /** Copy-on-read, safe against concurrent {@link #clearEntity}. */
    public Set<String> entityNames() {
        return new LinkedHashSet<String>(byEntity.keySet());
    }

    /** Iteration order unspecified; no-op for an unknown entity. Uses fastIterator() (reused entry view) to avoid allocating per entry. */
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

    /** Streaming bulk load: fill via {@link Loader#put}, finalize via {@link Loader#commit()}; an abandoned loader never reaches the live store. */
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

    /** Lazy min/max: inserts update incrementally, removing an extreme marks dirty and the next read rescans. */
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
