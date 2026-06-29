package app.l2nx.gs.adapter.api.kafka.sync.db.ban;

import app.l2nx.gs.adapter.api.kafka.commands.ban.WellKnownBanTargetTypes;
import app.l2nx.gs.adapter.api.kafka.commands.ban.WellKnownBanTypes;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one persisted ban row, payload of
 * {@code SyncEvent<BanDbDto>} on the platform-supplied per-tenant ban sync topic.
 * This is the up-channel mirror of the bans a host applies — whether issued from
 * the platform via {@link app.l2nx.gs.adapter.api.kafka.commands.ban.BanCommand}
 * or in-game (GM command, anti-cheat) — so the platform sees the full moderation
 * picture.
 *
 * <p>Only the primary key {@code id} is required; everything else is optional so
 * a host can surface the subset its schema carries. The schema provider maps its
 * engine's raw ban columns onto the platform-canonical vocabulary
 * ({@link WellKnownBanTypes} / {@link WellKnownBanTargetTypes}) in
 * {@code mapEntity}.</p>
 *
 * <p>A persisted row carries a concrete {@code targetType} — never the
 * {@code HARD} fan-out marker, which exists only on the inbound command.</p>
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

    /**
     * Primary key — the host ban row id, {@code NOT NULL}.
     */
    public long getId() {
        return id;
    }

    /**
     * Ban target dimension — a {@link WellKnownBanTargetTypes} value
     * ({@code CHARACTER} / {@code ACCOUNT} / {@code IP} / {@code HWID}; never
     * {@code HARD}). {@code null} when the host does not surface it.
     */
    public @Nullable String getTargetType() {
        return targetType;
    }

    /**
     * The keyed datum for {@link #getTargetType() targetType} — char id (as a
     * string), account login, plaintext IP, or HWID hash.
     */
    public @Nullable String getTargetValue() {
        return targetValue;
    }

    /**
     * Human-readable name of the target (character or account name), surfaced
     * because the in-game admin UI displayed it. {@code null} when not synced.
     */
    public @Nullable String getTargetName() {
        return targetName;
    }

    /**
     * Ban kind — a {@link WellKnownBanTypes} value. {@code null} when the
     * host does not surface it.
     */
    public @Nullable String getBanType() {
        return banType;
    }

    /**
     * Instant the ban lapses; {@code null} means permanent (the host's
     * "no expiry" sentinel maps to {@code null}).
     */
    public @Nullable Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * Human-readable ban reason. {@code null} when not synced.
     */
    public @Nullable String getReason() {
        return reason;
    }

    /**
     * Who issued the ban — an admin display name or service identifier.
     * {@code null} when not synced.
     */
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
