package app.l2nx.gs.adapter.api.kafka.commands.ban;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Inbound command instructing the game-server to apply a ban. Build-agnostic:
 * the contract names the target dimension, the ban kind, and the
 * expiry; the host maps those onto its own ban engine and decides how
 * the ban is enforced.
 *
 * <p>Reply:
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link BanResult}{@code >}
 * — carries the ids of the ban rows the host created (or matched), so
 * the platform can correlate the request with the rows that later arrive on the
 * db-sync stream. Common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — the targeted character / account does not exist.</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing {@code targetType},
 *     {@code targetValue}, or {@code banType}, or an unrecognized combination
 *     (Gson defaults a missing wire field to {@code null}; the handler MUST
 *     null-check before applying).</li>
 *     <li>{@code FORBIDDEN} — operation rejected on host policy grounds.</li>
 * </ul>
 *
 * <p><b>Target.</b> {@link #getTargetType() targetType} is a
 * {@link WellKnownBanTargetTypes} value and {@link #getTargetValue() targetValue}
 * carries the keyed datum (char id, account login, IP, or HWID hash). A
 * {@code HARD} target asks the host to fan one command out into the full
 * concrete set (character + account + IP + HWID) for the same subject; its
 * {@code targetValue} is the subject's char id, from which the host resolves the
 * other dimensions. Both REQUIRED.</p>
 *
 * <p><b>Kind.</b> {@link #getBanType() banType} is a {@link WellKnownBanTypes}
 * value naming what is restricted (game login, chat, party, jail). REQUIRED.</p>
 *
 * <p><b>Expiry.</b> {@link #isPermanent() permanent} drives the expiry: a
 * permanent ban never lapses and carries a {@code null} {@link #getExpiresAt()
 * expiresAt}; a non-permanent ban carries the {@code expiresAt} instant it lapses
 * at. The constructor enforces this invariant ({@code permanent ==
 * (expiresAt == null)}).</p>
 *
 * <p><b>Scope &amp; fan-out.</b> A {@code BanCommand} targets exactly one
 * server (routed by {@code Nx-Target-Server-Id}). Multi-server reach is a
 * platform concern: the platform issues one command per server in scope.</p>
 *
 * <p><b>Idempotency.</b> Applying a ban is naturally convergent — re-delivering
 * the same command re-asserts the same final ban state; the host SHOULD treat a
 * re-issued command for an already-active ban as a no-op success.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * Ban target dimension — a {@link WellKnownBanTargetTypes} value
     * ({@code UPPER_SNAKE} open string). REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when missing or unrecognized.
     */
    public String getTargetType() {
        return targetType;
    }

    /**
     * The keyed datum for {@link #getTargetType() targetType} — char id (as a
     * string), account login, plaintext IP, or HWID hash. REQUIRED.
     */
    public String getTargetValue() {
        return targetValue;
    }

    /**
     * Ban kind — a {@link WellKnownBanTypes} value ({@code UPPER_SNAKE}
     * open string). REQUIRED. Handler MUST emit {@code VALIDATION_FAILED} when
     * missing or unrecognized.
     */
    public String getBanType() {
        return banType;
    }

    /**
     * Whether the ban never lapses. When {@code true}, {@link #getExpiresAt()
     * expiresAt} is {@code null}; when {@code false}, {@code expiresAt} carries
     * the lapse instant.
     */
    public boolean isPermanent() {
        return permanent;
    }

    /**
     * Instant the ban lapses, or {@code null} for a {@link #isPermanent()
     * permanent} ban.
     */
    public @Nullable Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * Human-readable ban reason, surfaced to the player and stored on the
     * ban row. OPTIONAL.
     */
    public @Nullable String getReason() {
        return reason;
    }

    /**
     * Who issued the ban — an admin display name or service identifier, stored
     * on the ban row for audit. OPTIONAL.
     */
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
