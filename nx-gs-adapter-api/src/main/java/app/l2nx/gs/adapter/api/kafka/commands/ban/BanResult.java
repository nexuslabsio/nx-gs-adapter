package app.l2nx.gs.adapter.api.kafka.commands.ban;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Success payload of {@link BanCommand}: ids of the ban rows the host created or matched, so the platform can
 * correlate the request with rows arriving on the db-sync stream. A {@code HARD} fan-out returns one id per
 * dimension; a ban kind not persisted as an id-bearing row (e.g. shadow chat ban) returns an empty list.
 */
public final class BanResult {

    private final List<Long> banIds;

    public BanResult(List<Long> banIds) {
        this.banIds = banIds == null ? Collections.<Long>emptyList() : Collections.unmodifiableList(banIds);
    }

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
