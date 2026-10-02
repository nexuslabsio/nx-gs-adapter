package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.DroppedLine;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Success payload of {@link StartPrivateStoreSellCommand} / {@link StartPrivateStorePackageSellCommand}.
 */
public final class StartPrivateStoreResult {

    private final String storeType;
    private final int acceptedCount;
    private final List<DroppedLine> dropped;

    public StartPrivateStoreResult(String storeType, int acceptedCount, @Nullable List<DroppedLine> dropped) {
        if (storeType == null) {
            throw new IllegalArgumentException("storeType is required");
        }
        this.storeType = storeType;
        this.acceptedCount = acceptedCount;
        this.dropped = PrivateStoreLists.freeze(dropped);
    }

    /**
     * Host-defined open-string vocabulary (e.g. {@code "SELL"}, {@code "PACKAGE_SELL"}), not a closed adapter enum.
     */
    public String getStoreType() {
        return storeType;
    }

    public int getAcceptedCount() {
        return acceptedCount;
    }

    /** Empty when every requested line was accepted. */
    public List<DroppedLine> getDropped() {
        return dropped;
    }

    public Builder toBuilder() {
        return new Builder().storeType(storeType).acceptedCount(acceptedCount).dropped(dropped);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StartPrivateStoreResult)) return false;
        StartPrivateStoreResult that = (StartPrivateStoreResult) o;
        return acceptedCount == that.acceptedCount
                && Objects.equals(storeType, that.storeType)
                && Objects.equals(dropped, that.dropped);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeType, acceptedCount, dropped);
    }

    @Override
    public String toString() {
        return "StartPrivateStoreResult[storeType=" + storeType
                + ", acceptedCount=" + acceptedCount
                + ", dropped=" + dropped + "]";
    }

    public static final class Builder {
        private @Nullable String storeType;
        private int acceptedCount;
        private @Nullable List<DroppedLine> dropped;

        public Builder storeType(String storeType) {
            this.storeType = storeType;
            return this;
        }

        public Builder acceptedCount(int acceptedCount) {
            this.acceptedCount = acceptedCount;
            return this;
        }

        public Builder dropped(@Nullable List<DroppedLine> dropped) {
            this.dropped = dropped;
            return this;
        }

        public StartPrivateStoreResult build() {
            return new StartPrivateStoreResult(storeType, acceptedCount, dropped);
        }
    }
}
