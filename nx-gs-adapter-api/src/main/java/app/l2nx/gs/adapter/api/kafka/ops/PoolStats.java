package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.Objects;

/**
 * Snapshot of a JDBC connection-pool's active / idle / total / waiting counters.
 * Wire-shape value surfaced inside {@link ModuleStatus.Stats#getPool()} and
 * produced by Tier-3 SPI
 * {@code app.l2nx.gs.adapter.api.spi.JdbcConnectionSource#stats()}.
 *
 * <p>All fields are {@link Integer} (nullable) — pool implementations expose
 * different subsets of metrics. HikariCP / Tomcat JDBC / DBCP2 use the
 * {@code active}/{@code idle}/{@code total}/{@code waiting} naming convention;
 * legacy pools that only expose busy / idle counters leave {@code total} and
 * {@code waiting} {@code null}. Wire shape: integer fields are emitted as
 * numbers; absent fields are emitted as JSON {@code null}.</p>
 *
 * <p>Field semantics:</p>
 * <ul>
 *     <li>{@code active} — connections currently borrowed and in use.</li>
 *     <li>{@code idle} — connections in the pool but not borrowed.</li>
 *     <li>{@code total} — sum of {@code active} + {@code idle}; informational
 *     summary for pools that report it natively.</li>
 *     <li>{@code waiting} — threads blocked waiting for a connection. Nonzero
 *     here is a backpressure signal: the pool is saturated and consumers are
 *     queued.</li>
 * </ul>
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
