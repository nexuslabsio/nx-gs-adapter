package app.l2nx.gs.adapter.api.kafka.commands.ban;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTargetTypes;
import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTypes;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Applies a ban on the game-server. Build-agnostic: names the target dimension, ban kind and expiry; the host
 * maps them onto its own ban engine.
 *
 * <p>Reply: {@code CommandResult<BanResult>} with the ids of the ban rows created or matched. Errors:
 * {@code NOT_FOUND} (target missing), {@code VALIDATION_FAILED} (missing or unrecognized field; Gson leaves
 * missing wire fields {@code null}, so the handler must null-check), {@code FORBIDDEN} (host policy).</p>
 *
 * <p>A {@code HARD} target fans out into character + account + IP + HWID bans for the same subject; its
 * {@code targetValue} is the subject's char id, from which the host resolves the other dimensions.</p>
 *
 * <p>Invariant: {@code permanent == (expiresAt == null)}, enforced by the constructor.</p>
 *
 * <p>Targets exactly one server ({@code Nx-Target-Server-Id}); the platform issues one command per server in scope.
 * Re-delivery of a command for an already-active ban should be a no-op success.</p>
 */
public final class BanCommand implements NxCommand<BanResult> {

    private final String targetType;
    private final String targetValue;
    private final String banType;
    private final boolean permanent;
    private final @Nullable Instant expiresAt;
    private final @Nullable String reason;
    private final @Nullable String issuedBy;

    public BanCommand(
            String targetType,
            String targetValue,
            String banType,
            boolean permanent,
            @Nullable Instant expiresAt,
            @Nullable String reason,
            @Nullable String issuedBy) {
        if (targetType == null) {
            throw new IllegalArgumentException("targetType is required");
        }
        if (targetValue == null) {
            throw new IllegalArgumentException("targetValue is required");
        }
        if (banType == null) {
            throw new IllegalArgumentException("banType is required");
        }
        if (permanent && expiresAt != null) {
            throw new IllegalArgumentException("expiresAt must be null for a permanent ban");
        }
        if (!permanent && expiresAt == null) {
            throw new IllegalArgumentException("expiresAt is required for a non-permanent ban");
        }
        this.targetType = targetType;
        this.targetValue = targetValue;
        this.banType = banType;
        this.permanent = permanent;
        this.expiresAt = expiresAt;
        this.reason = reason;
        this.issuedBy = issuedBy;
    }

    /** A {@link WellKnownBanTargetTypes} value; handler emits {@code VALIDATION_FAILED} when missing or unrecognized. */
    public String getTargetType() {
        return targetType;
    }

    /** Char id (as a string), account login, plaintext IP, or HWID hash. */
    public String getTargetValue() {
        return targetValue;
    }

    /** A {@link WellKnownBanTypes} value; handler emits {@code VALIDATION_FAILED} when missing or unrecognized. */
    public String getBanType() {
        return banType;
    }

    public boolean isPermanent() {
        return permanent;
    }

    public @Nullable Instant getExpiresAt() {
        return expiresAt;
    }

    /** Surfaced to the player and stored on the ban row. */
    public @Nullable String getReason() {
        return reason;
    }

    /** Admin display name or service identifier, stored for audit. */
    public @Nullable String getIssuedBy() {
        return issuedBy;
    }

    public Builder toBuilder() {
        return new Builder()
                .targetType(targetType)
                .targetValue(targetValue)
                .banType(banType)
                .permanent(permanent)
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
        if (!(o instanceof BanCommand)) return false;
        BanCommand that = (BanCommand) o;
        return permanent == that.permanent
                && Objects.equals(targetType, that.targetType)
                && Objects.equals(targetValue, that.targetValue)
                && Objects.equals(banType, that.banType)
                && Objects.equals(expiresAt, that.expiresAt)
                && Objects.equals(reason, that.reason)
                && Objects.equals(issuedBy, that.issuedBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetType, targetValue, banType, permanent, expiresAt, reason, issuedBy);
    }

    @Override
    public String toString() {
        return "BanCommand[targetType=" + targetType
                + ", targetValue=" + targetValue
                + ", banType=" + banType
                + ", permanent=" + permanent
                + ", expiresAt=" + expiresAt
                + ", reason=" + reason
                + ", issuedBy=" + issuedBy + "]";
    }

    public static final class Builder {
        private String targetType;
        private String targetValue;
        private String banType;
        private boolean permanent;
        private @Nullable Instant expiresAt;
        private @Nullable String reason;
        private @Nullable String issuedBy;

        public Builder targetType(String targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder targetValue(String targetValue) {
            this.targetValue = targetValue;
            return this;
        }

        public Builder banType(String banType) {
            this.banType = banType;
            return this;
        }

        public Builder permanent(boolean permanent) {
            this.permanent = permanent;
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

        public BanCommand build() {
            return new BanCommand(targetType, targetValue, banType, permanent, expiresAt, reason, issuedBy);
        }
    }
}
