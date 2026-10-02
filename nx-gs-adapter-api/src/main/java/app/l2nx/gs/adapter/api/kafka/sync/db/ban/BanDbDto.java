package app.l2nx.gs.adapter.api.kafka.sync.db.ban;

import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTargetTypes;
import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTypes;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one persisted ban row, payload of {@code SyncEvent<BanDbDto>}. Mirrors bans from any
 * origin (platform {@link app.l2nx.gs.adapter.api.kafka.commands.ban.BanCommand} or in-game).
 *
 * Only {@code id} is required. Persisted rows carry a concrete {@code targetType}, never the
 * inbound-only {@code HARD} marker.
 */
public final class BanDbDto {

    private final long id;
    private final @Nullable String targetType;
    private final @Nullable String targetValue;
    private final @Nullable String targetName;
    private final @Nullable String banType;
    private final @Nullable Instant expiresAt;
    private final @Nullable String reason;
    private final @Nullable String issuedBy;

    public BanDbDto(
            long id,
            @Nullable String targetType,
            @Nullable String targetValue,
            @Nullable String targetName,
            @Nullable String banType,
            @Nullable Instant expiresAt,
            @Nullable String reason,
            @Nullable String issuedBy) {
        this.id = id;
        this.targetType = targetType;
        this.targetValue = targetValue;
        this.targetName = targetName;
        this.banType = banType;
        this.expiresAt = expiresAt;
        this.reason = reason;
        this.issuedBy = issuedBy;
    }

    public long getId() {
        return id;
    }

    /** A {@link WellKnownBanTargetTypes} value. */
    public @Nullable String getTargetType() {
        return targetType;
    }

    /** Char id (as string), account login, plaintext IP, or HWID hash, per {@link #getTargetType() targetType}. */
    public @Nullable String getTargetValue() {
        return targetValue;
    }

    public @Nullable String getTargetName() {
        return targetName;
    }

    /** A {@link WellKnownBanTypes} value. */
    public @Nullable String getBanType() {
        return banType;
    }

    /** Null means permanent. */
    public @Nullable Instant getExpiresAt() {
        return expiresAt;
    }

    public @Nullable String getReason() {
        return reason;
    }

    public @Nullable String getIssuedBy() {
        return issuedBy;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .targetType(targetType)
                .targetValue(targetValue)
                .targetName(targetName)
                .banType(banType)
                .expiresAt(expiresAt)
                .reason(reason)
                .issuedBy(issuedBy);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BanDbDto)) return false;
        BanDbDto that = (BanDbDto) o;
        return id == that.id
                && Objects.equals(targetType, that.targetType)
                && Objects.equals(targetValue, that.targetValue)
                && Objects.equals(targetName, that.targetName)
                && Objects.equals(banType, that.banType)
                && Objects.equals(expiresAt, that.expiresAt)
                && Objects.equals(reason, that.reason)
                && Objects.equals(issuedBy, that.issuedBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, targetType, targetValue, targetName, banType, expiresAt, reason, issuedBy);
    }

    @Override
    public String toString() {
        return "BanDbDto[id=" + id
                + ", targetType=" + targetType
                + ", targetValue=" + targetValue
                + ", targetName=" + targetName
                + ", banType=" + banType
                + ", expiresAt=" + expiresAt
                + ", reason=" + reason
                + ", issuedBy=" + issuedBy + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String targetType;
        private @Nullable String targetValue;
        private @Nullable String targetName;
        private @Nullable String banType;
        private @Nullable Instant expiresAt;
        private @Nullable String reason;
        private @Nullable String issuedBy;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder targetType(@Nullable String targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder targetValue(@Nullable String targetValue) {
            this.targetValue = targetValue;
            return this;
        }

        public Builder targetName(@Nullable String targetName) {
            this.targetName = targetName;
            return this;
        }

        public Builder banType(@Nullable String banType) {
            this.banType = banType;
            return this;
        }

        public Builder expiresAt(@Nullable Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Builder reason(@Nullable String reason) {
            this.reason = reason;
            return this;
        }

        public Builder issuedBy(@Nullable String issuedBy) {
            this.issuedBy = issuedBy;
            return this;
        }

        public BanDbDto build() {
            return new BanDbDto(id, targetType, targetValue, targetName, banType, expiresAt, reason, issuedBy);
        }
    }
}
