package app.l2nx.gs.adapter.api.kafka.events.leveldata;

import java.util.Objects;

/**
 * One level→required-exp row inside a {@link LevelExpTableSnapshotEvent}. Maps a
 * character level to the absolute experience total required to have reached it —
 * the cumulative EXP at the start of that level.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getLevel() level} — REQUIRED. Character level (1-based); the
 *   stable per-row key the consumer upserts on within a snapshot.</li>
 *   <li>{@link #getRequiredExp() requiredExp} — REQUIRED. Absolute (cumulative)
 *   experience total a character must have to be at {@link #getLevel() level}.
 *   Consumers derive "% progress within the current level" by combining a
 *   character's raw exp with the {@code requiredExp} of the current and next
 *   level:
 *   {@code pct = (exp - requiredExp[level]) / (requiredExp[level + 1] - requiredExp[level])}.</li>
 * </ul>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
 */
public final class LevelExpEntry {

    private final int level;
    private final long requiredExp;

    public LevelExpEntry(int level, long requiredExp) {
        this.level = level;
        this.requiredExp = requiredExp;
    }

    /**
     * Character level (1-based). The stable per-row key the platform upserts on
     * inside a snapshot.
     */
    public int getLevel() {
        return level;
    }

    /**
     * Absolute (cumulative) experience total required to be at
     * {@link #getLevel() level}.
     */
    public long getRequiredExp() {
        return requiredExp;
    }

    public Builder toBuilder() {
        return new Builder()
                .level(level)
                .requiredExp(requiredExp);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LevelExpEntry)) return false;
        LevelExpEntry that = (LevelExpEntry) o;
        return level == that.level
                && requiredExp == that.requiredExp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, requiredExp);
    }

    @Override
    public String toString() {
        return "LevelExpEntry[level=" + level
                + ", requiredExp=" + requiredExp + "]";
    }

    public static final class Builder {
        private int level;
        private long requiredExp;

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public Builder requiredExp(long requiredExp) {
            this.requiredExp = requiredExp;
            return this;
        }

        public LevelExpEntry build() {
            return new LevelExpEntry(level, requiredExp);
        }
    }
}
