package app.l2nx.gs.adapter.api.kafka.commands.sync;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Inbound command instructing the db-sync engine to force a re-sync of
 * selected rows of one entity: the snapshot hash of each targeted PK is
 * invalidated (a sentinel entry is inserted for a PK the snapshot never had,
 * so a platform ghost row gets a {@code DELETED} re-emit) and the next CDC
 * cycle re-publishes them. Pure adapter operation — no host game code
 * involved.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link ResyncRowsResult}{@code >}
 * — an <b>ack</b> sent after enqueue, carrying the per-entity invalidation
 * counts known at ack time. Per-entity completion follows asynchronously via
 * {@code ResyncCompletedEvent}. Common error replies:</p>
 * <ul>
 *     <li>{@code VALIDATION_FAILED} — missing {@code resyncId} /
 *     {@code entityName}, unknown {@code entityName}, or {@code pks}
 *     missing / empty / over {@link #MAX_PKS} / carrying a null or
 *     non-positive entry (object ids are strictly positive).</li>
 *     <li>{@code UNAVAILABLE} — the db-sync engine is not running.</li>
 * </ul>
 *
 * <p><b>Cascade.</b> When {@link #isCascade() cascade} is {@code true}, the
 * handler resolves — synchronously, before the ack — the rows of every
 * declared entity whose {@code parentRefs()} reference
 * {@link #getEntityName() entityName} ({@code SELECT <pk> FROM <table> WHERE
 * <fkColumn> IN (<pks>)}) and invalidates them alongside the requested rows.
 * Cascading from an entity nothing references is not an error — the result
 * then carries only the target entity.</p>
 *
 * <p><b>Required fields.</b> {@code resyncId}, {@code entityName}, and a
 * non-empty {@code pks} of at most {@link #MAX_PKS} entries — enforced by the
 * constructor for programmatic construction; wire-path Gson bypasses the
 * constructor, the handler re-checks and emits {@code VALIDATION_FAILED}.</p>
 *
 * <p><b>Partitioning.</b> Routed with a {@code null} partition key
 * (round-robin).</p>
 *
 * <p><b>Idempotency.</b> Redelivery merges into the same pending per-entity
 * invalidation set under the same {@code resyncId}; the platform sweep keyed
 * on the completion event is idempotent.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class ResyncRowsCommand implements NxCommand<ResyncRowsResult> {

    /**
     * Hard cap on {@link #getPks() pks} size. Keeps the command record well
     * under Kafka's default 1 MB and bounds the cascade {@code IN}-list
     * fan-out; larger repairs use {@link ResyncEntitiesCommand}.
     */
    public static final int MAX_PKS = 1000;

    private final UUID resyncId;
    private final String entityName;
    private final List<Long> pks;
    private final boolean cascade;

    public ResyncRowsCommand(UUID resyncId,
                             String entityName,
                             List<Long> pks,
                             boolean cascade) {
        if (resyncId == null) {
            throw new IllegalArgumentException("resyncId is required");
        }
        if (entityName == null) {
            throw new IllegalArgumentException("entityName is required");
        }
        if (pks == null || pks.isEmpty()) {
            throw new IllegalArgumentException("pks is required and must be non-empty");
        }
        if (pks.size() > MAX_PKS) {
            throw new IllegalArgumentException(
                    "pks must carry at most " + MAX_PKS + " entries (got " + pks.size() + ")");
        }
        this.resyncId = resyncId;
        this.entityName = entityName;
        this.pks = Collections.unmodifiableList(new ArrayList<Long>(pks));
        this.cascade = cascade;
    }

    /**
     * Platform-generated UUIDv7 identifying the resync operation. REQUIRED.
     * Echoed on every {@code ResyncCompletedEvent} the forced cycles emit.
     */
    public UUID getResyncId() {
        return resyncId;
    }

    /**
     * Target entity name as declared by the adapter's schema provider
     * ({@code EntityMapping.entityName()}). REQUIRED.
     */
    public String getEntityName() {
        return entityName;
    }

    /**
     * Primary keys to invalidate on the target entity. REQUIRED, non-empty,
     * at most {@link #MAX_PKS} entries. A PK absent from both the snapshot
     * and the host DB still produces a {@code DELETED} re-emit (sentinel
     * insert), repairing platform-side ghost rows.
     */
    public List<Long> getPks() {
        return pks;
    }

    /**
     * When {@code true}, also invalidate rows of every declared entity whose
     * {@code parentRefs()} reference {@link #getEntityName() entityName} and
     * whose FK matches one of {@link #getPks() pks}. Defaults to
     * {@code false} on the wire (Gson primitive default).
     */
    public boolean isCascade() {
        return cascade;
    }

    public Builder toBuilder() {
        return new Builder()
                .resyncId(resyncId)
                .entityName(entityName)
                .pks(pks)
                .cascade(cascade);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncRowsCommand)) return false;
        ResyncRowsCommand that = (ResyncRowsCommand) o;
        return cascade == that.cascade
                && resyncId.equals(that.resyncId)
                && entityName.equals(that.entityName)
                && pks.equals(that.pks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resyncId, entityName, pks, cascade);
    }

    @Override
    public String toString() {
        return "ResyncRowsCommand[resyncId=" + resyncId
                + ", entityName=" + entityName
                + ", pks=" + pks.size()
                + ", cascade=" + cascade + "]";
    }

    public static final class Builder {
        private @Nullable UUID resyncId;
        private @Nullable String entityName;
        private @Nullable List<Long> pks;
        private boolean cascade;

        public Builder resyncId(UUID resyncId) {
            this.resyncId = resyncId;
            return this;
        }

        public Builder entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder pks(List<Long> pks) {
            this.pks = pks;
            return this;
        }

        public Builder cascade(boolean cascade) {
            this.cascade = cascade;
            return this;
        }

        public ResyncRowsCommand build() {
            return new ResyncRowsCommand(resyncId, entityName, pks, cascade);
        }
    }
}
