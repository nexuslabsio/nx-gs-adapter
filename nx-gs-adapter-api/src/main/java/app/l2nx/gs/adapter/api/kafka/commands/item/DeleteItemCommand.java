package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Deletes {@code count} items from a stack in a character's inventory by item-instance object-id.
 *
 * <p>{@code NOT_FOUND}: character or item missing. {@code INVALID_STATE}: other owner, stack smaller than {@code count},
 * item locked or otherwise non-deletable. {@code FORBIDDEN}: policy. {@code VALIDATION_FAILED}: missing field or
 * non-positive count; Gson bypasses the constructor, so the handler must check.</p>
 *
 * <p>{@code count} equal to the stack size deletes the instance, less decrements it. Routed by {@code charId}.</p>
 *
 * <p>Delivery is at-most-once (see {@link app.l2nx.gs.adapter.api.spi.capability.CommandHandler}); a re-issue after a reply timeout may hit an already
 * decremented stack and looks like a fresh request, so the caller decides whether the first delete landed.</p>
 */
public final class DeleteItemCommand implements NxCommand<DeleteItemResult> {

    private final Long charId;
    private final Long itemId;
    private final Long count;

    public DeleteItemCommand(Long charId, Long itemId, Long count) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
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
        this.charId = charId;
        this.itemId = itemId;
        this.count = count;
    }

    public Long getCharId() {
        return charId;
    }

    /** Per-instance object-id, NOT the catalog item-template id. */
    public Long getItemId() {
        return itemId;
    }

    /** Must be positive; the builder defaults to {@code 1} but the wire must carry it explicitly. */
    public Long getCount() {
        return count;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).itemId(itemId).count(count);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteItemCommand)) return false;
        DeleteItemCommand that = (DeleteItemCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, itemId, count);
    }

    @Override
    public String toString() {
        return "DeleteItemCommand[charId=" + charId + ", itemId=" + itemId + ", count=" + count + "]";
    }

    public static final class Builder {
        private Long charId;
        private Long itemId;
        private Long count = 1L;

        public Builder charId(Long charId) {
            this.charId = charId;
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

        public DeleteItemCommand build() {
            return new DeleteItemCommand(charId, itemId, count);
        }
    }
}
