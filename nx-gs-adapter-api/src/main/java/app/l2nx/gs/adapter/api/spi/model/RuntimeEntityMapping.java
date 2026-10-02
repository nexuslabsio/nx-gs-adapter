package app.l2nx.gs.adapter.api.spi.model;

/**
 * One runtime-synced entity (character, party, ...): each tick snapshots live state and diffs
 * {@link #hash(Object)} against the previous tick to detect new and changed rows. PK is {@code long}.
 *
 * @param <T> wire DTO type for this entity
 */
public interface RuntimeEntityMapping<T> {

    /**
     * Singular domain name, key into {@code ConnectResponse.syncTopics.runtime}. May equal a
     * {@code db-sync} entity name: topics are namespaced ({@code db.*} vs {@code runtime.*}).
     */
    String entityName();

    /**
     * Gson serialization type; must be a concrete, non-parameterized class (erasure drops generics).
     */
    Class<T> dtoType();

    /**
     * Snapshot of live entities, called once per tick on the engine's daemon thread.
     * Must not block on locks held by hot game-server threads, and {@code next()} must be cheap.
     * Return a defensive copy when the source mutates concurrently: the engine iterates fully
     * before tick processing.
     */
    Iterable<RuntimeRow<T>> snapshot();

    /**
     * 64-bit change-detection hash, compared only via {@code ==}; algorithm is the provider's choice
     * (suggested: {@code app.l2nx.gs.commons.hash.Fnv1a64}). Quantize jittery fields before hashing
     * to avoid spurious CHANGED events.
     */
    long hash(T dto);
}
