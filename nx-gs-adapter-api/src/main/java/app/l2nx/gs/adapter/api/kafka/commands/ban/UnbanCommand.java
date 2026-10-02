package app.l2nx.gs.adapter.api.kafka.commands.ban;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTargetTypes;
import app.l2nx.gs.adapter.api.kafka.commands.ban.model.WellKnownBanTypes;
import java.util.Objects;

/**
 * Lifts a ban applied by {@link BanCommand}: same target dimension and ban kind, clears the matching ban(s).
 *
 * <p>Reply: {@code CommandResult<UnbanResult>}. {@code VALIDATION_FAILED} on missing fields, {@code FORBIDDEN} on host
 * policy. Clearing a ban that is not present is a no-op success ({@code removed = false}), not an error.
 * A {@code HARD} target clears every concrete dimension for the subject.</p>
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

    /** A {@link WellKnownBanTargetTypes} value. */
    public String getTargetType() {
        return targetType;
    }

    public String getTargetValue() {
        return targetValue;
    }

    /** A {@link WellKnownBanTypes} value. */
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
