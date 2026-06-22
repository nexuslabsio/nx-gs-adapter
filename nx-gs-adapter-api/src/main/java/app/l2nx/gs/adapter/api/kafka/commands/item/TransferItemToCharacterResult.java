package app.l2nx.gs.adapter.api.kafka.commands.item;

import java.util.Objects;

/**
 * Success payload of {@link TransferItemToCharacterCommand}. Echoes the actual amount
 * moved and both endpoints. The host's stack-size clamping semantics
 * mirror {@link DeleteItemResult}: {@link #getCountTransferred() countTransferred}
 * MAY be less than the inbound {@link TransferItemToCharacterCommand#getCount() count}
 * if the live stack was smaller at execution time.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class TransferItemToCharacterResult {

    private final Long itemId;
    private final Long countTransferred;
    private final Long fromCharId;
    private final Long toCharId;

    public TransferItemToCharacterResult(Long itemId, Long countTransferred, Long fromCharId, Long toCharId) {
        if (itemId == null) {
            throw new IllegalArgumentException("itemId is required");
        }
        if (countTransferred == null) {
            throw new IllegalArgumentException("countTransferred is required");
        }
        if (countTransferred <= 0L) {
            throw new IllegalArgumentException("countTransferred must be positive (got " + countTransferred + ")");
        }
        if (fromCharId == null) {
            throw new IllegalArgumentException("fromCharId is required");
        }
        if (toCharId == null) {
            throw new IllegalArgumentException("toCharId is required");
        }
        this.itemId = itemId;
        this.countTransferred = countTransferred;
        this.fromCharId = fromCharId;
        this.toCharId = toCharId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Long getCountTransferred() {
        return countTransferred;
    }

    public Long getFromCharId() {
        return fromCharId;
    }

    public Long getToCharId() {
        return toCharId;
    }

    public Builder toBuilder() {
        return new Builder()
                .itemId(itemId)
                .countTransferred(countTransferred)
                .fromCharId(fromCharId)
                .toCharId(toCharId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransferItemToCharacterResult)) return false;
        TransferItemToCharacterResult that = (TransferItemToCharacterResult) o;
        return Objects.equals(itemId, that.itemId)
                && Objects.equals(countTransferred, that.countTransferred)
                && Objects.equals(fromCharId, that.fromCharId)
                && Objects.equals(toCharId, that.toCharId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, countTransferred, fromCharId, toCharId);
    }

    @Override
    public String toString() {
        return "TransferItemToCharacterResult[itemId=" + itemId
                + ", countTransferred=" + countTransferred
                + ", fromCharId=" + fromCharId
                + ", toCharId=" + toCharId + "]";
    }

    public static final class Builder {
        private Long itemId;
        private Long countTransferred;
        private Long fromCharId;
        private Long toCharId;

        public Builder itemId(Long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder countTransferred(Long countTransferred) {
            this.countTransferred = countTransferred;
            return this;
        }

        public Builder fromCharId(Long fromCharId) {
            this.fromCharId = fromCharId;
            return this;
        }

        public Builder toCharId(Long toCharId) {
            this.toCharId = toCharId;
            return this;
        }

        public TransferItemToCharacterResult build() {
            return new TransferItemToCharacterResult(itemId, countTransferred, fromCharId, toCharId);
        }
    }
}
