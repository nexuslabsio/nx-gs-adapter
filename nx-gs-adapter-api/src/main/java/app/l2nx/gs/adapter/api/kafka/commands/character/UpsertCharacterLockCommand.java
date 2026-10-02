package app.l2nx.gs.adapter.api.kafka.commands.character;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Sets, replaces or clears a single character lock (IP / HWID / item-trade), mirroring the in-game voiced
 * {@code Security} command ({@code setVar(lockVar, value)} to set, {@code setVar(lockVar, 0)} to clear).
 *
 * <p>Exactly one {@code lockType} per call, so a command cannot touch any other lock; changing several locks takes
 * several commands. Reply: {@code CommandResult<UpsertCharacterLockResult>} with the post-upsert state. Errors:
 * {@code NOT_FOUND}, {@code VALIDATION_FAILED} (missing or unrecognized field; Gson leaves missing wire fields
 * {@code null}, so the handler must null-check), {@code FORBIDDEN}.</p>
 *
 * <p>{@code value} drives set-vs-clear: non-blank sets / replaces, {@code null} or blank clears (host writes the
 * {@code "0"} sentinel). Only {@code charId} and {@code lockType} are required. Routed by {@code charId}; writes an
 * absolute value, so redelivery converges on the same state.</p>
 */
public final class UpsertCharacterLockCommand implements NxCommand<UpsertCharacterLockResult> {

    private final Long charId;
    private final String lockType;
    private final @Nullable String value;

    public UpsertCharacterLockCommand(Long charId, String lockType, @Nullable String value) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (lockType == null) {
            throw new IllegalArgumentException("lockType is required");
        }
        this.charId = charId;
        this.lockType = lockType;
        this.value = value;
    }

    public Long getCharId() {
        return charId;
    }

    /** A {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes} value; handler emits {@code VALIDATION_FAILED} when missing or unrecognized. */
    public String getLockType() {
        return lockType;
    }

    /** {@code null} or blank clears the lock, a non-blank value sets / replaces it. */
    public @Nullable String getValue() {
        return value;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).lockType(lockType).value(value);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpsertCharacterLockCommand)) return false;
        UpsertCharacterLockCommand that = (UpsertCharacterLockCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(lockType, that.lockType)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, lockType, value);
    }

    @Override
    public String toString() {
        return "UpsertCharacterLockCommand[charId=" + charId + ", lockType=" + lockType + ", value=" + value + "]";
    }

    public static final class Builder {
        private Long charId;
        private String lockType;
        private @Nullable String value;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder lockType(String lockType) {
            this.lockType = lockType;
            return this;
        }

        public Builder value(@Nullable String value) {
            this.value = value;
            return this;
        }

        public UpsertCharacterLockCommand build() {
            return new UpsertCharacterLockCommand(charId, lockType, value);
        }
    }
}
