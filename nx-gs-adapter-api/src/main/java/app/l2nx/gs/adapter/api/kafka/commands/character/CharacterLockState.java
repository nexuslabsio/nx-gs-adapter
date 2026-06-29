package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Post-upsert state of a single character lock, carried inside
 * {@link UpsertCharacterLockResult}. Lets the caller see the truth for the one
 * lock the command named, without a follow-up read.
 *
 * <p>{@code lockType} is an {@code UPPER_SNAKE} open-string token — canonical
 * constants live in
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes}
 * ({@code IP} / {@code HWID} / {@code ITEM}). {@code active} is {@code true}
 * when the lock is in effect (the source value is present, non-blank, and not
 * the {@code "0"} sentinel); {@code value} carries the bound datum (plaintext
 * IP for an {@code IP} lock, HWID hash for {@code HWID} / {@code ITEM}),
 * {@code null} when the lock was cleared or carries no value.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * Lock kind — a
     * {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes}
     * value ({@code UPPER_SNAKE} open string), {@code NOT NULL}.
     */
    public String getLockType() {
        return lockType;
    }

    /**
     * {@code true} when the lock is in effect after the upsert; {@code false}
     * when it was cleared (or never set).
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Bound datum after the upsert — plaintext IP for an {@code IP} lock, the
     * HWID hash for {@code HWID} / {@code ITEM} locks. {@code null} when the
     * lock was cleared or carries no associated value.
     */
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
