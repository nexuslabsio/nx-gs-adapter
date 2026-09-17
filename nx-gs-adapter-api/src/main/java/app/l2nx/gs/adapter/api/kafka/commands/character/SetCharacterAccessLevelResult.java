package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Success payload of {@link SetCharacterAccessLevelCommand}.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class SetCharacterAccessLevelResult {

    private final Long charId;
    private final String accessLevel;
    private final @Nullable String previousAccessLevel;
    private final boolean wasOnline;

    public SetCharacterAccessLevelResult(
            Long charId, String accessLevel, @Nullable String previousAccessLevel, boolean wasOnline) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (accessLevel == null) {
            throw new IllegalArgumentException("accessLevel is required");
        }
        this.charId = charId;
        this.accessLevel = accessLevel;
        this.previousAccessLevel = previousAccessLevel;
        this.wasOnline = wasOnline;
    }

    public Long getCharId() {
        return charId;
    }

    /**
     * The level as the host stored it, in the same vocabulary (an int build
     * echoes the canonical numeric text).
     */
    public String getAccessLevel() {
        return accessLevel;
    }

    /**
     * The level before the write, {@code null} when the host could not read
     * it.
     */
    public @Nullable String getPreviousAccessLevel() {
        return previousAccessLevel;
    }

    /**
     * {@code true} when applied to a live session (full effect on next login, as
     * in-game); {@code false} when written to the offline row.
     */
    public boolean isWasOnline() {
        return wasOnline;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .accessLevel(accessLevel)
                .previousAccessLevel(previousAccessLevel)
                .wasOnline(wasOnline);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SetCharacterAccessLevelResult)) return false;
        SetCharacterAccessLevelResult that = (SetCharacterAccessLevelResult) o;
        return wasOnline == that.wasOnline
                && Objects.equals(charId, that.charId)
                && Objects.equals(accessLevel, that.accessLevel)
                && Objects.equals(previousAccessLevel, that.previousAccessLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, accessLevel, previousAccessLevel, wasOnline);
    }

    @Override
    public String toString() {
        return "SetCharacterAccessLevelResult[charId=" + charId + ", accessLevel=" + accessLevel
                + ", previousAccessLevel=" + previousAccessLevel + ", wasOnline=" + wasOnline + "]";
    }

    public static final class Builder {
        private Long charId;
        private String accessLevel;
        private @Nullable String previousAccessLevel;
        private boolean wasOnline;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder accessLevel(String accessLevel) {
            this.accessLevel = accessLevel;
            return this;
        }

        public Builder previousAccessLevel(@Nullable String previousAccessLevel) {
            this.previousAccessLevel = previousAccessLevel;
            return this;
        }

        public Builder wasOnline(boolean wasOnline) {
            this.wasOnline = wasOnline;
            return this;
        }

        public SetCharacterAccessLevelResult build() {
            return new SetCharacterAccessLevelResult(charId, accessLevel, previousAccessLevel, wasOnline);
        }
    }
}
