package app.l2nx.gs.adapter.api.kafka.commands.sync;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Forces a re-sync of selected rows of one entity: each PK's snapshot hash is invalidated (a sentinel entry is
 * inserted for a PK the snapshot never had, so a platform ghost row gets {@code DELETED}). Adapter-only.
 * Reply is an ack after enqueue with per-entity invalidation counts; completion follows via {@code ResyncCompletedEvent}.
 * Errors: {@code VALIDATION_FAILED} (missing/unknown {@code entityName}, {@code pks} missing/empty/over
 * {@link #MAX_PKS}/non-positive entry), {@code UNAVAILABLE} (engine not running).
 * With {@code cascade}, rows of entities whose {@code parentRefs()} reference {@code entityName} are resolved
 * synchronously before the ack and invalidated too. Partition key is {@code null}; redelivery merges under the same
 * {@code resyncId}.
 */
public final class ResyncRowsCommand implements NxCommand<ResyncRowsResult> {

    /** Keeps the record well under Kafka's default 1 MB and bounds the cascade {@code IN}-list; larger repairs use {@link ResyncEntitiesCommand}. */
    public static final int MAX_PKS = 1000;

    private final UUID resyncId;
    private final String entityName;
    private final List<Long> pks;
    private final boolean cascade;

    public ResyncRowsCommand(UUID resyncId, String entityName, List<Long> pks, boolean cascade) {
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

    /** Echoed on every {@code ResyncCompletedEvent} the forced cycles emit. */
    public UUID getResyncId() {
        return resyncId;
    }

    public String getEntityName() {
        return entityName;
    }

    /** A PK absent from both snapshot and host DB still yields a {@code DELETED} re-emit, repairing ghost rows. */
    public List<Long> getPks() {
        return pks;
    }

    /** Defaults to {@code false} on the wire (Gson primitive default). */
    public boolean isCascade() {
        return cascade;
    }

    public Builder toBuilder() {
        return new Builder().resyncId(resyncId).entityName(entityName).pks(pks).cascade(cascade);
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
