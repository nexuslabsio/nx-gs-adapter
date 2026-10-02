package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Post-upsert state of the one lock named by the command, carried in {@link UpsertCharacterLockResult} so the
 * caller needs no follow-up read. {@code active} means the source value is present, non-blank and not the
 * {@code "0"} sentinel.
 */
public final class CharacterLockState {

    private final String lockType;
    private final boolean active;
    private final @Nullable String value;

    public CharacterLockState(String lockType, boolean active, @Nullable String value) {
        this.lockType = Objects.requireNonNull(lockType, "lockType");
        this.active = active;
        this.value = value;
    }

    /** A {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes} value (open string). */
    public String getLockType() {
        return lockType;
    }

    public boolean isActive() {
        return active;
    }

    /** Plaintext IP for an {@code IP} lock, HWID hash for {@code HWID} / {@code ITEM}; {@code null} when cleared. */
    public @Nullable String getValue() {
        return value;
    }

    public Builder toBuilder() {
        return new Builder().lockType(lockType).active(active).value(value);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterLockState)) return false;
        CharacterLockState that = (CharacterLockState) o;
        return active == that.active && lockType.equals(that.lockType) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockType, active, value);
    }

    @Override
    public String toString() {
        return "CharacterLockState[lockType=" + lockType + ", active=" + active + ", value=" + value + "]";
    }

    public static final class Builder {
        private @Nullable String lockType;
        private boolean active;
        private @Nullable String value;

        public Builder lockType(String lockType) {
            this.lockType = lockType;
            return this;
        }

        public Builder active(boolean active) {
            this.active = active;
            return this;
        }

        public Builder value(@Nullable String value) {
            this.value = value;
            return this;
        }

        public CharacterLockState build() {
            return new CharacterLockState(lockType, active, value);
        }
    }
}
