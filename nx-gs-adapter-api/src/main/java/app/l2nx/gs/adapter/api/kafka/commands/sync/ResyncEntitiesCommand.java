package app.l2nx.gs.adapter.api.kafka.commands.sync;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Forces a full re-sync of whole entities: snapshot hashes are invalidated so the next CDC cycle re-publishes every
 * live row ({@code UPDATED}) and re-emits {@code DELETED} for snapshot-known ghosts. Adapter-only, no host game code.
 * Reply is an ack sent after enqueue; per-entity completion arrives later via {@code ResyncCompletedEvent}.
 * Errors: {@code VALIDATION_FAILED} (missing {@code resyncId} or any unknown entity name, no partial acceptance),
 * {@code UNAVAILABLE} (db-sync engine not running).
 * Partition key is {@code null} (round-robin). Redelivery merges pending requests per entity, so it is idempotent.
 */
public final class ResyncEntitiesCommand implements NxCommand<ResyncEntitiesResult> {

    private final UUID resyncId;
    private final List<String> entities;

    public ResyncEntitiesCommand(UUID resyncId, @Nullable List<String> entities) {
        if (resyncId == null) {
            throw new IllegalArgumentException("resyncId is required");
        }
        this.resyncId = resyncId;
        this.entities = entities == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(entities));
    }

    /** Echoed on every {@code ResyncCompletedEvent} the forced cycles emit. */
    public UUID getResyncId() {
        return resyncId;
    }

    /**
     * Null/empty = ALL declared db-sync entities. Any unknown name fails the command with {@code VALIDATION_FAILED}.
     * Wire-path Gson bypasses the constructor, so handlers treat {@code null} and empty identically.
     */
    public List<String> getEntities() {
        return entities;
    }

    public Builder toBuilder() {
        return new Builder().resyncId(resyncId).entities(entities);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResyncEntitiesCommand)) return false;
        ResyncEntitiesCommand that = (ResyncEntitiesCommand) o;
        return resyncId.equals(that.resyncId) && entities.equals(that.entities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resyncId, entities);
    }

    @Override
    public String toString() {
        return "ResyncEntitiesCommand[resyncId=" + resyncId + ", entities=" + entities + "]";
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
