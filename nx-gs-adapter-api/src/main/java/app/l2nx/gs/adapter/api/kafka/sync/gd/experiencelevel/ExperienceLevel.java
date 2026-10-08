package app.l2nx.gs.adapter.api.kafka.sync.gd.experiencelevel;

import java.util.Objects;

/** One row of the experience table; {@code requiredExp} is cumulative, not per-level. */
public final class ExperienceLevel {

    private final int level;
    private final long requiredExp;

    public ExperienceLevel(int level, long requiredExp) {
        this.level = level;
        this.requiredExp = requiredExp;
    }

    public int getLevel() {
        return level;
    }

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
        if (!(o instanceof ExperienceLevel)) return false;
        ExperienceLevel that = (ExperienceLevel) o;
        return level == that.level && requiredExp == that.requiredExp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, requiredExp);
    }

    @Override
    public String toString() {
        return "ExperienceLevel[level=" + level + ", requiredExp=" + requiredExp + "]";
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

        public ExperienceLevel build() {
            return new ExperienceLevel(level, requiredExp);
        }
    }
}
