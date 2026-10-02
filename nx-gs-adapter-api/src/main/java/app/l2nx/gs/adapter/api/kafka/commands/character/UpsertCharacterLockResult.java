package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;

/** Post-upsert state of the one lock the command named, so the platform needs no follow-up read. */
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

    public Long getCharId() {
        return charId;
    }

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
