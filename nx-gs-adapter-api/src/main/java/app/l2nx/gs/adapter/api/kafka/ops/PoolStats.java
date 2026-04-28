package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.Objects;

/**
 * Snapshot of a connection-pool's busy / idle / total counters. Wire-shape value
 * surfaced inside {@link ModuleStatus.Stats#getPool()} and produced by Tier-3 SPI
 * {@code app.l2nx.gs.adapter.api.spi.JdbcConnectionSource#stats()}.
 *
 * <p>{@code total} is nullable — some pool implementations expose only busy/idle
 * counters. Wire shape: integer fields are emitted as numbers; absent {@code total}
 * is emitted as JSON {@code null}.</p>
 */
public final class PoolStats {

    private final int busy;
    private final int idle;
    private final Integer total;

    public PoolStats(int busy, int idle, Integer total) {
        this.busy = busy;
        this.idle = idle;
        this.total = total;
    }

    public int getBusy() {
        return busy;
    }

    public int getIdle() {
        return idle;
    }

    public Integer getTotal() {
        return total;
    }

    public Builder toBuilder() {
        return new Builder().busy(busy).idle(idle).total(total);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PoolStats)) return false;
        PoolStats that = (PoolStats) o;
        return busy == that.busy
                && idle == that.idle
                && Objects.equals(total, that.total);
    }

    @Override
    public int hashCode() {
        return Objects.hash(busy, idle, total);
    }

    @Override
    public String toString() {
        return "PoolStats[busy=" + busy + ", idle=" + idle + ", total=" + total + "]";
    }

    public static final class Builder {
        private int busy;
        private int idle;
        private Integer total;

        public Builder busy(int busy) {
            this.busy = busy;
            return this;
        }

        public Builder idle(int idle) {
            this.idle = idle;
            return this;
        }

        public Builder total(Integer total) {
            this.total = total;
            return this;
        }

        public PoolStats build() {
            return new PoolStats(busy, idle, total);
        }
    }
}
