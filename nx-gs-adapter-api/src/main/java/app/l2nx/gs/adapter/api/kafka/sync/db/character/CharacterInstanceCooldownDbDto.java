package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import java.time.Instant;
import java.util.Objects;

/**
 * Wire DTO for one row of {@code character_instance_time} (or its tenant
 * equivalent), carried inside {@link CharacterDbDto#getInstanceCooldowns()}.
 *
 * <p>Surfaces the per-instance re-entry cooldown: {@code instanceId} (which
 * instance/reflection) and {@code reentryAt} — the absolute UTC moment the
 * character may re-enter. The source column is an absolute epoch-millis
 * deadline (not a duration); schema providers map it via
 * {@code Instant.ofEpochMilli(time)}.</p>
 *
 * <p>An expired cooldown (a {@code reentryAt} in the past) may linger on the
 * source side until the game server prunes it at character login — platform
 * consumers treat a past {@code reentryAt} as "no active cooldown".</p>
 */
public final class CharacterInstanceCooldownDbDto {

    private final int instanceId;
    private final Instant reentryAt;

    public CharacterInstanceCooldownDbDto(int instanceId, Instant reentryAt) {
        this.instanceId = instanceId;
        this.reentryAt = Objects.requireNonNull(reentryAt, "reentryAt");
    }

    /**
     * Instance / reflection identifier — {@code NOT NULL} on the source side.
     * Resolved to a readable name on the platform via the {@code gd_instances}
     * catalog (gd-sync {@code instance} entity).
     */
    public int getInstanceId() {
        return instanceId;
    }

    /**
     * Absolute UTC re-entry deadline — derived from the source absolute
     * epoch-millis {@code time} column via {@code Instant.ofEpochMilli(time)}.
     */
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
