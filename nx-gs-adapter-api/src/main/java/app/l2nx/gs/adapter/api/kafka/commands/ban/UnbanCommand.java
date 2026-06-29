package app.l2nx.gs.adapter.api.kafka.commands.ban;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to lift a previously applied ban.
 * The inverse of {@link BanCommand}: it names the same target dimension and
 * ban kind and asks the host to clear the matching ban(s).
 *
 * <p>Reply:
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link UnbanResult}{@code >}
 * — reports whether anything was removed and the ids of the cleared ban
 * rows. Common error replies:</p>
 * <ul>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing {@code targetType},
 *     {@code targetValue}, or {@code banType}.</li>
 *     <li>{@code FORBIDDEN} — operation rejected on host policy grounds.</li>
 * </ul>
 *
 * <p>Clearing a ban that is not present is a no-op success ({@code removed =
 * false}), not an error — the post-condition (no such ban) already holds.</p>
 *
 * <p><b>Target.</b> {@link #getTargetType() targetType}
 * ({@link WellKnownBanTargetTypes}) + {@link #getTargetValue() targetValue}
 * identify the subject; {@link #getBanType() banType}
 * ({@link WellKnownBanTypes}) names which ban kind to clear. All
 * REQUIRED. A {@code HARD} target clears every concrete dimension for the
 * subject.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class UnbanCommand implements NxCommand<UnbanResult> {

    private final String targetType;
    private final String targetValue;
    private final String banType;

    public UnbanCommand(String targetType, String targetValue, String banType) {
        if (targetType == null) {
            throw new IllegalArgumentException("targetType is required");
        }
        if (targetValue == null) {
            throw new IllegalArgumentException("targetValue is required");
        }
        if (banType == null) {
            throw new IllegalArgumentException("banType is required");
        }
        this.targetType = targetType;
        this.targetValue = targetValue;
        this.banType = banType;
    }

    /**
     * Ban target dimension to clear — a {@link WellKnownBanTargetTypes} value.
     * REQUIRED.
     */
    public String getTargetType() {
        return targetType;
    }

    /**
     * The keyed datum for {@link #getTargetType() targetType}. REQUIRED.
     */
    public String getTargetValue() {
        return targetValue;
    }

    /**
     * Ban kind to clear — a {@link WellKnownBanTypes} value. REQUIRED.
     */
    public String getBanType() {
        return banType;
    }

    public Builder toBuilder() {
        return new Builder().targetType(targetType).targetValue(targetValue).banType(banType);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UnbanCommand)) return false;
        UnbanCommand that = (UnbanCommand) o;
        return Objects.equals(targetType, that.targetType)
                && Objects.equals(targetValue, that.targetValue)
                && Objects.equals(banType, that.banType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetType, targetValue, banType);
    }

    @Override
    public String toString() {
        return "UnbanCommand[targetType=" + targetType + ", targetValue=" + targetValue + ", banType=" + banType + "]";
    }

    public static final class Builder {
        private String targetType;
        private String targetValue;
        private String banType;

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

        public UnbanCommand build() {
            return new UnbanCommand(targetType, targetValue, banType);
        }
    }
}
