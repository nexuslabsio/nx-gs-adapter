package app.l2nx.gs.db.sync.engine;

import it.unimi.dsi.fastutil.longs.*;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory primitive-keyed CRC32 snapshot, one {@link Long2IntAVLTreeMap} per
 * synced entity. The engine uses this to detect created / updated / deleted PKs
 * by comparing the previous-cycle snapshot against the current Phase-1 scan.
 *
 * <p>AVL tree (rather than open-hash) so {@link #keysInRange} runs in
 * {@code O(log N + k)} via {@code subMap} instead of an O(N) full-map walk —
 * critical for the items table at 12M rows × N windows per cycle.</p>
 *
 * <p>Sentinel: {@code defaultReturnValue()} stays at the fastutil default (0).
 * Callers must use {@link #containsCrc(String, long)} before reading via
 * {@link #getCrc(String, long)} when 0 is a legitimate CRC32 value (it is).</p>
 *
 * <p>Thread safety: NOT safe for concurrent mutation. The engine spins one
 * scheduler thread per entity and ticks a single entity-task at a time; all
 * mutations for a given entity happen on its own thread.</p>
 */
public final class SnapshotStore {

    private final Map<String, Long2IntAVLTreeMap> byEntity = new HashMap<String, Long2IntAVLTreeMap>();

    public int getCrc(String entityName, long pk) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        if (map == null) {
            return 0;
        }
        return map.get(pk);
    }

    public boolean containsCrc(String entityName, long pk) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        return map != null && map.containsKey(pk);
    }

    public void putCrc(String entityName, long pk, int crc) {
        mapOf(entityName).put(pk, crc);
    }

    public void removeCrc(String entityName, long pk) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        if (map != null) {
            map.remove(pk);
        }
    }

    /**
     * Returns the PKs in {@code [fromPk, toPk]} (closed interval, matching
     * {@code WHERE pk BETWEEN ? AND ?}) currently stored for the entity. Used
     * by the diff stage to know which previously-seen PKs fall in the just-scanned
     * window — a PK in this set but missing from the current scan = DELETED.
     *
     * <p>Range derivation:</p>
     * <ul>
     *     <li>{@code toPk < Long.MAX_VALUE}: {@code subMap(fromPk, toPk + 1)} —
     *     fastutil's {@code subMap} upper bound is exclusive.</li>
     *     <li>{@code toPk == Long.MAX_VALUE}: {@code tailMap(fromPk)} so the
     *     {@code +1} doesn't overflow.</li>
     * </ul>
     */
    public LongSet keysInRange(String entityName, long fromPk, long toPk) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return new LongOpenHashSet();
        }
        Long2IntSortedMap rangeView;
        if (toPk == Long.MAX_VALUE) {
            rangeView = map.tailMap(fromPk);
        } else {
            rangeView = map.subMap(fromPk, toPk + 1L);
        }
        LongSortedSet rangeKeys = rangeView.keySet();
        LongOpenHashSet result = new LongOpenHashSet(rangeKeys.size());
        LongIterator it = rangeKeys.iterator();
        while (it.hasNext()) {
            result.add(it.nextLong());
        }
        return result;
    }

    public int sizeOf(String entityName) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        return map == null ? 0 : map.size();
    }

    public void clearEntity(String entityName) {
        Long2IntAVLTreeMap map = byEntity.remove(entityName);
        if (map != null) {
            map.clear();
        }
    }

    public void clearAll() {
        for (Long2IntAVLTreeMap map : byEntity.values()) {
            map.clear();
        }
        byEntity.clear();
    }

    private Long2IntAVLTreeMap mapOf(String entityName) {
        Long2IntAVLTreeMap map = byEntity.get(entityName);
        if (map == null) {
            map = new Long2IntAVLTreeMap();
            byEntity.put(entityName, map);
        }
        return map;
    }
}
