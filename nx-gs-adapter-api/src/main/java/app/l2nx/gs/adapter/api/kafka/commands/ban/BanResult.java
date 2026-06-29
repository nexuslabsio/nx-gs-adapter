package app.l2nx.gs.adapter.api.kafka.commands.ban;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link BanCommand}. Carries the ids of the ban rows
 * the host created or matched while applying the ban, so the platform can
 * correlate the request with the rows that subsequently arrive on the db-sync
 * stream.
 *
 * <p>A {@code HARD} fan-out returns one id per concrete dimension. A ban kind
 * that the host does not persist as an id-bearing ban row (e.g. a
 * char-variable-backed shadow chat ban) returns an empty list — the host still
 * surfaces it on the sync stream through its own entity.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class BanResult {

    private final List<Long> banIds;

    public BanResult(List<Long> banIds) {
        this.banIds = banIds == null ? Collections.<Long>emptyList() : Collections.unmodifiableList(banIds);
    }

    /**
     * Ids of the ban rows created or matched by this ban. Never
     * {@code null}; empty when the ban kind is not persisted as an id-bearing
     * row.
     */
    public List<Long> getBanIds() {
        return banIds;
    }

    public Builder toBuilder() {
        return new Builder().banIds(banIds);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BanResult)) return false;
        BanResult that = (BanResult) o;
        return banIds.equals(that.banIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(banIds);
    }

    @Override
    public String toString() {
        return "BanResult[banIds=" + banIds + "]";
    }

    public static final class Builder {
        private List<Long> banIds;

        public Builder banIds(List<Long> banIds) {
            this.banIds = banIds;
            return this;
        }

        public BanResult build() {
            return new BanResult(banIds);
        }
    }
}
