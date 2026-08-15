package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to delete a quantity of items
 * from a character's inventory by item-instance object-id. Replaces the
 * legacy {@code com.bohpts.messaging.dto.item.DeleteItemRequestV1} (which
 * had no count and always deleted the full stack).
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <Void>}
 * — {@code success()} on a successful delete; common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — character or item-instance does not exist.</li>
 *     <li>{@code INVALID_STATE} — item belongs to a different character, the
 *     stack has fewer items than {@code count}, the item is locked (in trade,
 *     in private store, equipped on a frozen character …), or otherwise
 *     non-deletable in its current state.</li>
 *     <li>{@code FORBIDDEN} — operation rejected on policy grounds (e.g.
 *     deleting from a banned admin's inventory is disallowed by the host's
 *     audit policy).</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing a required field
 *     (Gson defaults the boxed {@code Long} to {@code null} on a missing wire
 *     field; handler MUST check non-null before applying).</li>
 * </ul>
 *
 * <p><b>Identity.</b> {@link #getItemId() itemId} is the L2
 * object-id of the specific item instance (NOT the catalog item-template id) —
 * unique per game-server lifetime, identifies one stack.</p>
 *
 * <p><b>Quantity semantics.</b> {@link #getCount() count} is the number of
 * items to delete from the stack. Builder defaults to {@code 1} (matches the
 * common single-item delete case). When {@code count} equals the stack size
 * the entire instance is deleted; when less, the stack is decremented.
 * {@code count <= 0} is rejected at construction (programmatic use) and
 * SHOULD be rejected by the handler with {@code VALIDATION_FAILED} on the
 * wire path.</p>
 *
 * <p><b>Required fields.</b> All three fields ({@code charId},
 * {@code itemId}, {@code count}) are semantically REQUIRED. The
 * constructor enforces non-null values via {@link IllegalArgumentException}
 * for programmatic construction (tests, host-side replays). Wire-path
 * deserialization bypasses the constructor via Gson — the handler is
 * responsible for null-checking and emitting {@code VALIDATION_FAILED} when
 * a wire field is missing.</p>
 *
 * <p><b>Partitioning.</b> Routed by {@link #getCharId() charId} on the
 * commands topic — sequential with other character-scoped operations on the
 * same character.</p>
 *
 * <p><b>Re-issue safety.</b> Delivery is at-most-once (see
 * {@link app.l2nx.gs.adapter.api.spi.CommandHandler}); what repeats is a caller re-issuing after a
 * reply timeout. The stack may already have been decremented, and the handler cannot tell that
 * apart from a fresh request — deciding whether the delete landed is the caller's job.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * Owning character's primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field
     * (boxed {@code Long} surfaces missing wire data as {@code null}).
     */
    public Long getCharId() {
        return charId;
    }

    /**
     * L2 object-id of the item instance to delete. REQUIRED. NOT the catalog
     * item-template id — this is the per-instance unique id. Handler MUST
     * emit {@code VALIDATION_FAILED} on missing wire data.
     */
    public Long getItemId() {
        return itemId;
    }

    /**
     * Number of items to delete from the stack. REQUIRED, MUST be positive.
     * Builder defaults to {@code 1}; the wire MUST carry the field
     * explicitly. Handler MUST emit {@code VALIDATION_FAILED} when missing
     * or non-positive.
     */
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

        /**
         * Override the default count of 1.
         */
        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public DeleteItemCommand build() {
            return new DeleteItemCommand(charId, itemId, count);
        }
    }
}
