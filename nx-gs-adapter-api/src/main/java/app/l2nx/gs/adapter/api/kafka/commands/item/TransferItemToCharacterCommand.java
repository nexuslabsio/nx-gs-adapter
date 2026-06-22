package app.l2nx.gs.adapter.api.kafka.commands.item;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to transfer a stack of items
 * from one character to another by item-instance object-id. Covers all four
 * online/offline source-target combinations transparently to the caller —
 * the handler routes internally based on the live state of the from / to
 * characters.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <Void>}
 * — {@code success()} on a successful transfer; common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — source character, target character, or item
 *     instance does not exist.</li>
 *     <li>{@code INVALID_STATE} — item not in a transferable location
 *     (auction, freight, lease, private store, equipped on a frozen
 *     character …), target inventory full / over weight cap, source stack
 *     has fewer items than {@code count}, or otherwise rejected by the
 *     host's transfer policy in the current state.</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing a required field
 *     (Gson defaults boxed {@code Long} to {@code null} on missing wire
 *     field; handler MUST check non-null before applying).</li>
 *     <li>{@code INTERNAL_ERROR} — unexpected error during persistence
 *     (clone insert, owner reassign, …).</li>
 * </ul>
 *
 * <p><b>Identity.</b> {@link #getItemId() itemId} is the L2
 * object-id of the specific item instance (NOT the catalog item-template id) —
 * unique per game-server lifetime, identifies one stack. Source character
 * is identified by {@link #getCharIdFrom() charIdFrom}, target by
 * {@link #getCharIdTo() charIdTo}.</p>
 *
 * <p><b>Quantity semantics.</b> {@link #getCount() count} is the number of
 * items to move from the source stack to the target. Builder defaults to
 * {@code 1}. For stackable items: when {@code count} equals the stack size
 * the entire instance moves; when less, the stack is split (a partial stack
 * is created on the target, the source stack is decremented). For
 * non-stackable items {@code count} MUST be {@code 1} — handlers MUST
 * reject other values with {@code VALIDATION_FAILED} or {@code INVALID_STATE}.
 * {@code count <= 0} is rejected at construction (programmatic use); on the
 * wire path handler MUST emit {@code VALIDATION_FAILED} on missing /
 * non-positive values.</p>
 *
 * <p><b>Required fields.</b> All four fields ({@code charIdFrom},
 * {@code charIdTo}, {@code itemId}, {@code count}) are semantically
 * REQUIRED. The constructor enforces non-null values via
 * {@link IllegalArgumentException} for programmatic construction (tests,
 * host-side replays). Wire-path deserialization bypasses the constructor
 * via Gson — the handler is responsible for null-checking and emitting
 * {@code VALIDATION_FAILED} when a wire field is missing.</p>
 *
 * <p><b>Partitioning.</b> Routed by {@link #getCharIdFrom() charIdFrom} on
 * the commands topic — sequential with other source-character-scoped
 * operations on the same character. Note: this does NOT serialize against
 * concurrent commands keyed by {@code charIdTo} — handlers MUST not assume
 * exclusive access to the target.</p>
 *
 * <p><b>Idempotency.</b> Handler MUST be idempotent — Kafka redelivery on
 * crash recovery may re-invoke the handler with the same {@code
 * Nx-Correlation-Id}. Best practice: check the inbound correlation id
 * against a recently-processed cache; if matched, treat as already-applied
 * and reply success without re-moving the items.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
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

    /**
     * Source character's primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field.
     */
    public Long getCharIdFrom() {
        return charIdFrom;
    }

    /**
     * Target character's primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field.
     */
    public Long getCharIdTo() {
        return charIdTo;
    }

    /**
     * L2 object-id of the item instance to transfer. REQUIRED. NOT the
     * catalog item-template id — this is the per-instance unique id.
     * Handler MUST emit {@code VALIDATION_FAILED} on missing wire data.
     */
    public Long getItemId() {
        return itemId;
    }

    /**
     * Number of items to transfer from the source stack. REQUIRED, MUST be
     * positive. Builder defaults to {@code 1}. Handler MUST emit
     * {@code VALIDATION_FAILED} when missing or non-positive.
     */
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

        /**
         * Override the default count of 1.
         */
        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public TransferItemToCharacterCommand build() {
            return new TransferItemToCharacterCommand(charIdFrom, charIdTo, itemId, count);
        }
    }
}
