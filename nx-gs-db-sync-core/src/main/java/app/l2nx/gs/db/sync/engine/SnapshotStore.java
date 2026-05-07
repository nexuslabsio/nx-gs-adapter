package app.l2nx.gs.db.sync.engine;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * In-memory primitive-keyed CRC32 snapshot, one {@link Long2IntOpenHashMap} per
 * synced entity. The engine uses this to detect created / updated / deleted PKs
 * by comparing the previous-cycle snapshot against the current Phase-1 scan.
 *
 * <p>Open-hash map (rather than AVL tree) keeps the per-entry footprint at
 * ~16 bytes — critical at 6.5M+ items, where an AVL tree would otherwise
 * burn ~360 MB on snapshot alone. Range / extreme lookups become O(N) scans
 * (per-window {@link #keysInRange} and per-cycle {@link #minPk} /
 * {@link #maxPk}) on dedicated daemon threads — the trade is heap for CPU,
 * and on a 4 GB host heap is the constraint.</p>
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

    private final Map<String, Long2IntOpenHashMap> byEntity = new HashMap<String, Long2IntOpenHashMap>();

    public int getCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null) {
            return 0;
        }
        return map.get(pk);
    }

    public boolean containsCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        return map != null && map.containsKey(pk);
    }

    public void putCrc(String entityName, long pk, int crc) {
        mapOf(entityName).put(pk, crc);
    }

    public void removeCrc(String entityName, long pk) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map != null) {
            map.remove(pk);
        }
    }

    /**
     * Returns the PKs in {@code [fromPk, toPk]} (closed interval, matching
     * {@code WHERE pk BETWEEN ? AND ?}) currently stored for the entity. Used
     * by the diff stage to know which previously-seen PKs fall in the
     * just-scanned window — a PK in this set but missing from the current scan
     * = DELETED.
     *
     * <p>Open-hash backing means this is an O(N) full-keys scan filtered by
     * range. Called once per window per cycle; per-window result is small (one
     * window's worth of PKs) so transient memory stays bounded by
     * {@code rowsPerWindow}, not by total snapshot size.</p>
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

    public int sizeOf(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        return map == null ? 0 : map.size();
    }

    /**
     * Smallest PK currently held in the snapshot for the entity, or empty if
     * the entity has no entries (initial cold cycle, or every previously-seen
     * PK has been published as a tombstone).
     *
     * <p>Open-hash backing forces an O(N) scan; called once per cycle by
     * {@code WindowPlanner} for envelope planning (cdc-engine R2: the
     * partitioned range covers the union of the live DB range AND the
     * snapshot's range so deletion of the row at the current {@code MIN(pk)}
     * still falls inside some next-cycle window).</p>
     */
    public OptionalLong minPk(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return OptionalLong.empty();
        }
        long min = Long.MAX_VALUE;
        LongIterator it = map.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            if (pk < min) {
                min = pk;
            }
        }
        return OptionalLong.of(min);
    }

    /**
     * Largest PK currently held in the snapshot for the entity, or empty if
     * the entity has no entries. Symmetric to {@link #minPk}; together they
     * bound the snapshot's PK envelope for the {@code WindowPlanner}
     * (cdc-engine R2). Same O(N) cost — single scan per cycle.
     */
    public OptionalLong maxPk(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null || map.isEmpty()) {
            return OptionalLong.empty();
        }
        long max = Long.MIN_VALUE;
        LongIterator it = map.keySet().iterator();
        while (it.hasNext()) {
            long pk = it.nextLong();
            if (pk > max) {
                max = pk;
            }
        }
        return OptionalLong.of(max);
    }

    public void clearEntity(String entityName) {
        Long2IntOpenHashMap map = byEntity.remove(entityName);
        if (map != null) {
            map.clear();
        }
    }

    public void clearAll() {
        for (Long2IntOpenHashMap map : byEntity.values()) {
            map.clear();
        }
        byEntity.clear();
    }

    private Long2IntOpenHashMap mapOf(String entityName) {
        Long2IntOpenHashMap map = byEntity.get(entityName);
        if (map == null) {
            map = new Long2IntOpenHashMap();
            byEntity.put(entityName, map);
        }
        return map;
    }
}
