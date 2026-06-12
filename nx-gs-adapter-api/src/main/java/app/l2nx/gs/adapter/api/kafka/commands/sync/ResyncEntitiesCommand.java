package app.l2nx.gs.adapter.api.kafka.commands.sync;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Inbound command instructing the db-sync engine to force a full re-sync of
 * whole entities: every snapshot hash of each targeted entity is invalidated
 * so the next CDC cycle re-publishes every live row (as {@code UPDATED}) and
 * re-emits {@code DELETED} for snapshot-known ghosts. Pure adapter operation
 * — no host game code involved.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link ResyncEntitiesResult}{@code >}
 * — an <b>ack</b> sent after the invalidation requests are enqueued; it does
 * NOT wait for the forced cycles. Per-entity completion is signalled later
 * via {@code ResyncCompletedEvent} on the {@code sync} events family. Common
 * error replies:</p>
 * <ul>
 *     <li>{@code VALIDATION_FAILED} — missing {@code resyncId}, or any name
 *     in a non-empty {@link #getEntities() entities} list is not a declared
 *     db-sync entity (no partial acceptance).</li>
 *     <li>{@code UNAVAILABLE} — the db-sync engine is not running (module
 *     disabled / failed / not started yet).</li>
 * </ul>
 *
 * <p><b>Required fields.</b> {@link #getResyncId() resyncId} is REQUIRED —
 * the constructor enforces non-null for programmatic construction. Wire-path
 * deserialization bypasses the constructor via Gson — the handler re-checks
 * and emits {@code VALIDATION_FAILED} on missing wire data.</p>
 *
 * <p><b>Partitioning.</b> Routed with a {@code null} partition key
 * (round-robin) — resync carries no per-character ordering need.</p>
 *
 * <p><b>Idempotency.</b> Redelivery re-enqueues the same {@code resyncId};
 * the engine merges pending requests per entity and emits one completion
 * event per drained {@code resyncId}, so a duplicate delivery converges to
 * the same platform-visible outcome (the platform sweep is idempotent).</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class ResyncEntitiesCommand implements NxCommand<ResyncEntitiesResult> {

    private final UUID resyncId;
    private final List<String> entities;

    public ResyncEntitiesCommand(UUID resyncId,
                                 @Nullable List<String> entities) {
        if (resyncId == null) {
            throw new IllegalArgumentException("resyncId is required");
        }
        this.resyncId = resyncId;
        this.entities = entities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(entities));
    }

    /**
     * Platform-generated UUIDv7 identifying the resync operation. REQUIRED.
     * Echoed on every {@code ResyncCompletedEvent} the forced cycles emit so
     * the platform can attribute completion to the issuing operation.
     */
    public UUID getResyncId() {
        return resyncId;
    }

    /**
     * Entity names to resync. Null/empty (wire or constructor) = ALL db-sync
     * entities declared by the adapter's schema provider. A non-empty list
     * containing any unknown name fails the whole command with
     * {@code VALIDATION_FAILED} — no partial acceptance. Non-null on read;
     * {@code null} passed to the constructor normalizes to an empty list
     * (wire-path Gson bypasses the constructor — handlers treat {@code null}
     * and empty identically).
     */
    public List<String> getEntities() {
        return entities;
    }

    public Builder toBuilder() {
        return new Builder()
                .resyncId(resyncId)
                .entities(entities);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncEntitiesCommand)) return false;
        ResyncEntitiesCommand that = (ResyncEntitiesCommand) o;
        return resyncId.equals(that.resyncId)
                && entities.equals(that.entities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resyncId, entities);
    }

    @Override
    public String toString() {
        return "ResyncEntitiesCommand[resyncId=" + resyncId
                + ", entities=" + entities + "]";
    }

    public static final class Builder {
        private @Nullable UUID resyncId;
        private @Nullable List<String> entities;

        public Builder resyncId(UUID resyncId) {
            this.resyncId = resyncId;
            return this;
        }

        public Builder entities(@Nullable List<String> entities) {
            this.entities = entities;
            return this;
        }

        public ResyncEntitiesCommand build() {
            return new ResyncEntitiesCommand(resyncId, entities);
        }
    }
}
