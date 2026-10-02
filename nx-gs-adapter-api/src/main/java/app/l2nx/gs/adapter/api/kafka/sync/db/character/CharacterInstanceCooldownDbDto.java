package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import java.time.Instant;
import java.util.Objects;

/**
 * Wire DTO for one instance re-entry cooldown, carried in {@link CharacterDbDto#getInstanceCooldowns()}.
 * {@code reentryAt} is an absolute deadline; a past value may linger until login pruning and means no active cooldown.
 */
public final class CharacterInstanceCooldownDbDto {

    private final int instanceId;
    private final Instant reentryAt;

    public CharacterInstanceCooldownDbDto(int instanceId, Instant reentryAt) {
        this.instanceId = instanceId;
        this.reentryAt = Objects.requireNonNull(reentryAt, "reentryAt");
    }

    /** Resolved to a name via the {@code gd_instances} catalog. */
    public int getInstanceId() {
        return instanceId;
    }

    public Instant getReentryAt() {
        return reentryAt;
    }

    public Builder toBuilder() {
        return new Builder().instanceId(instanceId).reentryAt(reentryAt);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterInstanceCooldownDbDto)) return false;
        CharacterInstanceCooldownDbDto that = (CharacterInstanceCooldownDbDto) o;
        return instanceId == that.instanceId && Objects.equals(reentryAt, that.reentryAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, reentryAt);
    }

    @Override
    public String toString() {
        return "CharacterInstanceCooldownDbDto[instanceId=" + instanceId + ", reentryAt=" + reentryAt + "]";
    }

    public static final class Builder {
        private int instanceId;
        private Instant reentryAt;

        public Builder instanceId(int instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder reentryAt(Instant reentryAt) {
            this.reentryAt = reentryAt;
            return this;
        }

        public CharacterInstanceCooldownDbDto build() {
            return new CharacterInstanceCooldownDbDto(instanceId, reentryAt);
        }
    }
}
