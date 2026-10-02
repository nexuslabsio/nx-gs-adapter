package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Moves a stack of items between characters by item-instance object-id; the handler routes internally across all
 * online/offline combinations.
 *
 * <p>{@code NOT_FOUND}: character or item missing. {@code INVALID_STATE}: non-transferable location, target full or
 * over weight, source stack smaller than {@code count}. {@code VALIDATION_FAILED}: missing field or non-positive
 * count; Gson bypasses the constructor, so the handler must check. {@code INTERNAL_ERROR}: persistence failure.</p>
 *
 * <p>Stackables: {@code count} equal to the stack size moves the instance, less splits it. Non-stackables require
 * {@code count == 1}; others are rejected with {@code VALIDATION_FAILED} or {@code INVALID_STATE}.</p>
 *
 * <p>Routed by {@code charIdFrom}, which does NOT serialize against commands keyed by {@code charIdTo}; handlers
 * must not assume exclusive access to the target.</p>
 *
 * <p>Delivery is at-most-once (see {@link app.l2nx.gs.adapter.api.spi.capability.CommandHandler}); a re-issue after a reply timeout may find the items already
 * moved and looks like a fresh request, so the caller decides whether the first transfer landed.</p>
 */
public final class TransferItemToCharacterCommand implements NxCommand<TransferItemToCharacterResult> {

    private final Long charIdFrom;
    private final Long charIdTo;
    private final Long itemId;
    private final Long count;

    public TransferItemToCharacterCommand(Long charIdFrom, Long charIdTo, Long itemId, Long count) {
        if (charIdFrom == null) {
            throw new IllegalArgumentException("charIdFrom is required");
        }
        if (charIdTo == null) {
            throw new IllegalArgumentException("charIdTo is required");
        }
        if (itemId == null) {
            throw new IllegalArgumentException("itemId is required");
        }
        if (count == null) {
            throw new IllegalArgumentException("count is required");
        }
        if (count <= 0L) {
            throw new IllegalArgumentException("count must be positive (got " + count + ")");
        }
        this.charIdFrom = charIdFrom;
        this.charIdTo = charIdTo;
        this.itemId = itemId;
        this.count = count;
    }

    public Long getCharIdFrom() {
        return charIdFrom;
    }

    public Long getCharIdTo() {
        return charIdTo;
    }

    /** Per-instance object-id, NOT the catalog item-template id. */
    public Long getItemId() {
        return itemId;
    }

    /** Must be positive; the builder defaults to {@code 1}. Non-stackables require {@code 1}. */
    public Long getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder()
                .charIdFrom(charIdFrom)
                .charIdTo(charIdTo)
                .itemId(itemId)
                .count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransferItemToCharacterCommand)) return false;
        TransferItemToCharacterCommand that = (TransferItemToCharacterCommand) o;
        return Objects.equals(charIdFrom, that.charIdFrom)
                && Objects.equals(charIdTo, that.charIdTo)
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charIdFrom, charIdTo, itemId, count);
    }

    @Override
    public String toString() {
        return "TransferItemToCharacterCommand[charIdFrom=" + charIdFrom
                + ", charIdTo=" + charIdTo
                + ", itemId=" + itemId
                + ", count=" + count + "]";
    }

    public static final class Builder {
        private Long charIdFrom;
        private Long charIdTo;
        private Long itemId;
        private Long count = 1L;

        public Builder charIdFrom(Long charIdFrom) {
            this.charIdFrom = charIdFrom;
            return this;
        }

        public Builder charIdTo(Long charIdTo) {
            this.charIdTo = charIdTo;
            return this;
        }

        public Builder itemId(Long itemId) {
            this.itemId = itemId;
            return this;
        }

        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public TransferItemToCharacterCommand build() {
            return new TransferItemToCharacterCommand(charIdFrom, charIdTo, itemId, count);
        }
    }
}
