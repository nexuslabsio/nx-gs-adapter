package app.l2nx.gs.adapter.api.spi;

/**
 * Tier-2 SPI: describes ONE runtime-synced entity (character, party, …). The
 * runtime engine ticks once per declared mapping, snapshots live state via
 * {@link #snapshot()}, hashes each row via {@link #hash(Object)}, and diffs
 * against the previous tick's hash map to detect NEW / CHANGED rows.
 *
 * <p>No DB-schema concepts here — runtime SPI is pure-Java, host-internal.
 * Compare with {@link EntityMapping} (DB-side counterpart): same identity
 * shape ({@code entityName} + {@code dtoType}), different production model
 * ({@link #snapshot()} vs JDBC {@code ResultSet}).</p>
 *
 * <p>PK is {@code long} end-to-end — consistent with {@code db-sync}
 * conventions and Kafka message-key serialization
 * ({@code LongSerializer}).</p>
 *
 * @param <T> wire DTO type for this entity
 */
public interface RuntimeEntityMapping<T> {

    /**
     * Domain identifier in singular form: {@code "character"}, {@code "party"},
     * {@code "raid_boss"}. Used as the lookup key into
     * {@code ConnectResponse.syncTopics.runtime} to resolve the Kafka topic
     * for this entity. Surfaced through heartbeat as {@code EntityStats.name}.
     *
     * <p>Entity name MAY collide with a {@code db-sync} entity of the same
     * name — namespace separation in {@code SyncTopics} ({@code db.character}
     * vs {@code runtime.character}) is sufficient.</p>
     */
    String entityName();

    /**
     * DTO class for serialization. The engine passes this to Gson when
     * serializing the {@code SyncEvent<T>} payload. Platform consumers compile
     * against the same class for their {@code Consumer<SyncEvent<T>>}.
     *
     * <p><b>Constraint:</b> the DTO MUST be a concrete, non-parameterized class.
     * {@link Class} loses generic parameters through erasure.</p>
     */
    Class<T> dtoType();

    /**
     * Produce a one-shot snapshot of currently-live entities. Called once per
     * tick on the engine's daemon thread.
     *
     * <p><b>Implementation contract:</b></p>
     * <ul>
     *     <li>MUST NOT block on locks held by hot game-server threads.</li>
     *     <li>{@code next()} on the returned iterator MUST be cheap — read
     *     field values from the live game object, do not recompute / refetch.</li>
     *     <li>SHOULD return a defensive copy of the underlying live collection
     *     (e.g. {@code new ArrayList<>(world.getAllPlayers())}) when the source
     *     mutates concurrently — the engine iterates fully and then releases
     *     the iterable before tick processing.</li>
     * </ul>
     *
     * <p>Each {@link RuntimeRow} carries the primary key (entity identity) and
     * the typed DTO populated from live field accessors at snapshot time.</p>
     */
    Iterable<RuntimeRow<T>> snapshot();

    /**
     * Compute a 64-bit hash over the DTO fields the operator wants to track for
     * change detection. Engine treats the returned long as <b>opaque</b> — it
     * is compared via {@code ==} only; the algorithm is the provider's choice.
     *
     * <p>Recommended default: <b>FNV-1a 64-bit</b> via
     * {@code app.l2nx.gs.commons.hash.Fnv1a64} (published in
     * {@code :nx-gs-commons}). Faster than CRC32 in pure Java (mul+xor vs
     * table lookup), 64-bit collision safety (~0% on 10k entries vs ~1% for
     * 32-bit CRC).</p>
     *
     * <p>Provider author controls which fields participate. High-frequency
     * micro-jitter (e.g. coordinate sub-pixel updates) can be quantized
     * before hashing so it does not trigger spurious CHANGED events.</p>
     */
    long hash(T dto);
}
