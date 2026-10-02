package app.l2nx.gs.adapter.api.kafka.ops.model;

import java.util.Objects;

/**
 * JDBC pool counters inside {@link ModuleStatus.Stats#getPool()}. All fields are nullable and emitted as JSON {@code null} when the pool does not expose them.
 * A nonzero {@code waiting} means the pool is saturated.
 */
public final class PoolStats {

    private final Integer active;
    private final Integer idle;
    private final Integer total;
    private final Integer waiting;

    public PoolStats(Integer active, Integer idle, Integer total, Integer waiting) {
        this.active = active;
        this.idle = idle;
        this.total = total;
        this.waiting = waiting;
    }

    public Integer getActive() {
        return active;
    }

    public Integer getIdle() {
        return idle;
    }

    public Integer getTotal() {
        return total;
    }

    public Integer getWaiting() {
        return waiting;
    }

    public Builder toBuilder() {
        return new Builder().active(active).idle(idle).total(total).waiting(waiting);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PoolStats)) return false;
        PoolStats that = (PoolStats) o;
        return Objects.equals(active, that.active)
                && Objects.equals(idle, that.idle)
                && Objects.equals(total, that.total)
                && Objects.equals(waiting, that.waiting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(active, idle, total, waiting);
    }

    @Override
    public String toString() {
        return "PoolStats[active=" + active + ", idle=" + idle + ", total=" + total + ", waiting=" + waiting + "]";
    }

    public static final class Builder {
        private Integer active;
        private Integer idle;
        private Integer total;
        private Integer waiting;

        public Builder active(Integer active) {
            this.active = active;
            return this;
        }

        public Builder idle(Integer idle) {
            this.idle = idle;
            return this;
        }

        public Builder total(Integer total) {
            this.total = total;
            return this;
        }

        public Builder waiting(Integer waiting) {
            this.waiting = waiting;
            return this;
        }

        public PoolStats build() {
            return new PoolStats(active, idle, total, waiting);
        }
    }
}
