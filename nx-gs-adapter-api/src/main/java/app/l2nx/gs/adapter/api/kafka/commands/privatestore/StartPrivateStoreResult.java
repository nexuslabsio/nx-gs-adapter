package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Success payload of {@link StartPrivateStoreSellCommand} /
 * {@link StartPrivateStorePackageSellCommand}.
 *
 * <p><b>Empty-list semantics.</b> {@link #getDropped() dropped} is non-null
 * on read; {@code null} passed to the constructor is normalized to
 * {@link Collections#emptyList()}. An empty list signals every requested
 * {@link SellLine} was accepted.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; defensive copy in
 * constructor; unmodifiable list view from the getter.</p>
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
     * Open-string store-type token the host opened (e.g. {@code "SELL"} /
     * {@code "PACKAGE_SELL"}); host-defined vocabulary, not a closed adapter
     * enum. REQUIRED.
     */
    public String getStoreType() {
        return storeType;
    }

    /**
     * Number of requested lines the host actually listed.
     */
    public int getAcceptedCount() {
        return acceptedCount;
    }

    /**
     * Requested lines the host rejected when opening the store. Non-null;
     * empty when every line was accepted.
     */
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
