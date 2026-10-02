package app.l2nx.gs.adapter.api.kafka.events.leveldata;

import java.util.Objects;

/**
 * Level to required-exp row of a {@link LevelExpTableSnapshotEvent}: the cumulative EXP at the start of that level.
 *
 * <p>Progress within a level is derived by the consumer:
 * {@code pct = (exp - requiredExp[level]) / (requiredExp[level + 1] - requiredExp[level])}.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} preserves constructor parameter names so Jackson / Gson bind without
 * {@code @JsonProperty}.</p>
 */
public final class LevelExpEntry {

    private final int level;
    private final long requiredExp;

    public LevelExpEntry(int level, long requiredExp) {
        this.level = level;
        this.requiredExp = requiredExp;
    }

    /**
     * Character level, 1-based; the per-row key the consumer upserts on.
     */
    public int getLevel() {
        return level;
    }

    /**
     * Cumulative experience required to be at {@link #getLevel() level}.
     */
    public long getRequiredExp() {
        return requiredExp;
    }

    public Builder toBuilder() {
        return new Builder().level(level).requiredExp(requiredExp);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LevelExpEntry)) return false;
        LevelExpEntry that = (LevelExpEntry) o;
        return level == that.level && requiredExp == that.requiredExp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, requiredExp);
    }

    @Override
    public String toString() {
        return "LevelExpEntry[level=" + level + ", requiredExp=" + requiredExp + "]";
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
