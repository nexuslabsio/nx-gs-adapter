package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;

/**
 * Success payload of {@link UpsertCharacterLockCommand}. Echoes the
 * post-upsert state of the one lock the command named, so the platform sees
 * the truth that lands on the host without a follow-up read.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class UpsertCharacterLockResult {

    private final Long charId;
    private final CharacterLockState lock;

    public UpsertCharacterLockResult(Long charId, CharacterLockState lock) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (lock == null) {
            throw new IllegalArgumentException("lock is required");
        }
        this.charId = charId;
        this.lock = lock;
    }

    /**
     * Target character's primary key the upsert was applied to.
     */
    public Long getCharId() {
        return charId;
    }

    /**
     * Final state of the affected lock — type, active flag, and value after
     * the set/clear.
     */
    public CharacterLockState getLock() {
        return lock;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).lock(lock);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpsertCharacterLockResult)) return false;
        UpsertCharacterLockResult that = (UpsertCharacterLockResult) o;
        return Objects.equals(charId, that.charId) && Objects.equals(lock, that.lock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, lock);
    }

    @Override
    public String toString() {
        return "UpsertCharacterLockResult[charId=" + charId + ", lock=" + lock + "]";
    }

    public static final class Builder {
        private Long charId;
        private CharacterLockState lock;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder lock(CharacterLockState lock) {
            this.lock = lock;
            return this;
        }

        public UpsertCharacterLockResult build() {
            return new UpsertCharacterLockResult(charId, lock);
        }
    }
}
