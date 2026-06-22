package app.l2nx.gs.adapter.api.spi;

import java.util.Collection;

/**
 * Out-of-band sync trigger — host calls {@link #requestNow} after mutating
 * entity state (item transfer, account change, mail attach, …) so the next
 * sync pass for the affected entity runs immediately instead of waiting up
 * to the next scheduled tick. Acquired via {@link ConnectContext#sync()} or
 * {@link CommandContext#sync()}.
 *
 * <p>{@link #requestNow} MUST NOT block, throw, or propagate failure.
 * Unknown entity (no trigger registered) is a no-op + DEBUG log.</p>
 *
 * <p>{@link #registerTrigger} is the module-side hook bound by sync
 * modules during their connect lifecycle — last-write-wins on the
 * entity name.</p>
 */
public interface NxSync {

    void requestNow(String entityName, long pk);

    void requestNow(String entityName, Collection<Long> pks);

    /**
     * In-process, per-command force-resync of the given primary keys of one
     * entity. Unlike {@link #requestNow} (a CRC-diff cycle that re-publishes
     * only changed rows), this GUARANTEES re-publication of the named rows on
     * the next immediate cycle regardless of whether their content changed —
     * the sync module perturbs each row's snapshot hash so the diff classifies
     * it as a publish. Use it from a command handler right after a mutation
     * whose row content may be byte-identical to the prior snapshot yet still
     * needs to re-emit (e.g. a side-effect the platform must observe).
     *
     * <p>When {@code cascade} is {@code true}, child rows that declare this
     * entity as their parent via {@link EntityMapping#parentRefs()} are
     * force-republished too (for {@code entityName="character"}, every item with
     * {@code owner_id} in {@code pks}), so the platform receives a consistent
     * re-publication of the rows and everything hanging off them.</p>
     *
     * <p>Fire-and-forget: MUST NOT block, throw, or propagate failure. The
     * caller's thread (game thread or Kafka consumer thread) only hands off the
     * request — cascade resolution and snapshot perturbation run on adapter
     * pools, never on the caller. This is an INTERNAL per-command resync, NOT a
     * platform-tracked admin resync operation: no completion signal
     * ({@code ResyncCompletedEvent}) is emitted for it. Unknown entity (no
     * handler registered) is a no-op + DEBUG log; {@code null}/empty {@code pks}
     * is a no-op.</p>
     *
     * @param entityName entity name as declared by {@link EntityMapping#entityName()}
     * @param pks        primary keys to force-republish; {@code null}/empty = no-op
     * @param cascade    also force-republish child rows declared via
     *                   {@link EntityMapping#parentRefs()}
     */
    void requestResync(String entityName, Collection<Long> pks, boolean cascade);

    void registerTrigger(String entityName, NxSyncTrigger trigger);

    /**
     * Module-side hook bound by the sync module during its connect lifecycle —
     * routes {@link #requestResync(String, Collection, boolean)} calls into the
     * engine's no-event pk-republish path (and the cascade fan-out). Last
     * registration wins. A context wired without a sync runtime, or before the
     * module registers, drops resync requests silently.
     */
    void registerResyncHandler(NxSyncResyncHandler handler);
}
