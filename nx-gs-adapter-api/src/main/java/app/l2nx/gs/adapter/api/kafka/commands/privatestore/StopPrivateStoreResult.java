package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.Objects;

/**
 * Success payload of {@link StopPrivateStoreCommand}.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class StopPrivateStoreResult {

    private final String previousStoreType;

    public StopPrivateStoreResult(String previousStoreType) {
        if (previousStoreType == null) {
            throw new IllegalArgumentException("previousStoreType is required");
        }
        this.previousStoreType = previousStoreType;
    }

    /**
     * Open-string store-type token that was open before this command closed
     * it (e.g. {@code "SELL"} / {@code "PACKAGE_SELL"} / {@code "BUY"});
     * host-defined vocabulary, not a closed adapter enum.
     */
    public String getPreviousStoreType() {
        return previousStoreType;
    }

    public Builder toBuilder() {
        return new Builder().previousStoreType(previousStoreType);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StopPrivateStoreResult)) return false;
        StopPrivateStoreResult that = (StopPrivateStoreResult) o;
        return Objects.equals(previousStoreType, that.previousStoreType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previousStoreType);
    }

    @Override
    public String toString() {
        return "StopPrivateStoreResult[previousStoreType=" + previousStoreType + "]";
    }

    public static final class Builder {
        private String previousStoreType;

        public Builder previousStoreType(String previousStoreType) {
            this.previousStoreType = previousStoreType;
            return this;
        }

        public StopPrivateStoreResult build() {
            return new StopPrivateStoreResult(previousStoreType);
        }
    }
}
