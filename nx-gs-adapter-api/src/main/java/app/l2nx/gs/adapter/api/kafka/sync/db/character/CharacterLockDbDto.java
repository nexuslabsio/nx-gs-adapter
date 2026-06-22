package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one active character lock, carried inside
 * {@link CharacterDbDto#getLocks()}.
 *
 * <p>Surfaces a single active binding derived from a build-specific
 * {@code character_variables} row ({@code lockIp} / {@code lockHwid} /
 * {@code lockItem} on bohpts). The schema provider emits one entry per
 * <b>active</b> lock — a lock is active iff its source value is present,
 * non-blank, and not the {@code "0"} sentinel; inactive locks are dropped before
 * assembly, so every entry that reaches the wire is an in-effect lock.</p>
 *
 * <p>{@code lockType} is an {@code UPPER_SNAKE} open-string token (canonical
 * constants in {@link WellKnownCharacterLockTypes}); {@code lockValue} carries
 * the bound datum (plaintext IP for {@code IP}, HWID hash for {@code HWID} /
 * {@code ITEM}), surfaced because the legacy admin UI displayed it.</p>
 */
public final class CharacterLockDbDto {

    private final String lockType;
    private final @Nullable String lockValue;

    public CharacterLockDbDto(String lockType, @Nullable String lockValue) {
        this.lockType = Objects.requireNonNull(lockType, "lockType");
        this.lockValue = lockValue;
    }

    /**
     * Lock kind — an {@link WellKnownCharacterLockTypes} value ({@code UPPER_SNAKE}
     * open string), {@code NOT NULL} on the wire.
     */
    public String getLockType() {
        return lockType;
    }

    /**
     * The bound datum — plaintext IP for an {@code IP} lock, the 64-hex HWID hash
     * for {@code HWID} / {@code ITEM} locks. {@code null} when the host surfaces the
     * lock without an associated value.
     */
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
