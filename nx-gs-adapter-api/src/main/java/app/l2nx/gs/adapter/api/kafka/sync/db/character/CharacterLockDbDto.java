package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one active character lock, carried in {@link CharacterDbDto#getLocks()}.
 * Only in-effect locks are emitted (value present, non-blank, not {@code "0"}).
 */
public final class CharacterLockDbDto {

    private final String lockType;
    private final @Nullable String lockValue;

    public CharacterLockDbDto(String lockType, @Nullable String lockValue) {
        this.lockType = Objects.requireNonNull(lockType, "lockType");
        this.lockValue = lockValue;
    }

    /** A {@link WellKnownCharacterLockTypes} value (open string). */
    public String getLockType() {
        return lockType;
    }

    /** Plaintext IP for {@code IP}, 64-hex HWID hash for {@code HWID}/{@code ITEM}; null when the host gives none. */
    public @Nullable String getLockValue() {
        return lockValue;
    }

    public Builder toBuilder() {
        return new Builder().lockType(lockType).lockValue(lockValue);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterLockDbDto)) return false;
        CharacterLockDbDto that = (CharacterLockDbDto) o;
        return lockType.equals(that.lockType) && Objects.equals(lockValue, that.lockValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockType, lockValue);
    }

    @Override
    public String toString() {
        return "CharacterLockDbDto[lockType=" + lockType + ", lockValue=" + lockValue + "]";
    }

    public static final class Builder {
        private @Nullable String lockType;
        private @Nullable String lockValue;

        public Builder lockType(String lockType) {
            this.lockType = lockType;
            return this;
        }

        public Builder lockValue(@Nullable String lockValue) {
            this.lockValue = lockValue;
            return this;
        }

        public CharacterLockDbDto build() {
            return new CharacterLockDbDto(lockType, lockValue);
        }
    }
}
