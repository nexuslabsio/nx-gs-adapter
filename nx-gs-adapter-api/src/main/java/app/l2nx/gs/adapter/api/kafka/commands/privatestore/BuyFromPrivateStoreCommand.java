package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Inbound command instructing the game-server to buy the given lots from
 * another character's open sell-store on behalf of {@code buyerCharId} —
 * the remote ("buy now") counterpart of the in-game store purchase packet.
 * The buyer does NOT have to be online, in range, or in the same world
 * instance as the seller.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link BuyFromPrivateStoreResult}{@code >}.
 * Common error replies:</p>
 * <ul>
 *     <li>{@code VALIDATION_FAILED} — malformed {@link #getLines() lines}, or
 *     buyer and seller are the same character.</li>
 *     <li>{@code NOT_FOUND} — the seller is not in the world or has no open
 *     sell-store.</li>
 *     <li>{@code INVALID_STATE} — the lot no longer matches the request, the
 *     store type is not served, or the buyer cannot receive the goods (adena,
 *     weight, slots, regulated combat, …).</li>
 *     <li>{@code FORBIDDEN} — the buyer is barred from trading at all
 *     (security lock, cursed weapon, restricted account).</li>
 *     <li>{@code COMMAND_EXPIRED} — {@link #getDeadline() deadline} has
 *     already passed when the host picked up the command; nothing moved.</li>
 * </ul>
 *
 * <p><b>Machine-readable reject reason.</b> Every non-OK reply carries a
 * stable {@code reason} code in
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandProblem#getExtensions()
 * CommandProblem.extensions}, with the numeric context of that reason
 * (required vs available adena / slots / weight) in sibling extension keys.
 * The platform localizes the code; the host never sends player-facing text.</p>
 *
 * <p><b>All-or-nothing.</b> Either every line is bought at exactly the
 * requested count and price, or nothing is charged and nothing moves. The host
 * validates all lots against the seller's live trade list <em>before</em>
 * entering the engine, so a stale order book fails the command instead of
 * silently buying less than the caller saw.</p>
 *
 * <p><b>Required fields.</b> {@link #getBuyerCharId() buyerCharId},
 * {@link #getSellerCharId() sellerCharId}, a non-empty {@link #getLines()
 * lines} and {@link #getDeadline() deadline} are REQUIRED; buyer and seller
 * MUST differ. The constructor enforces this via
 * {@link IllegalArgumentException} for programmatic construction. Wire-path
 * deserialization bypasses the constructor — the handler re-checks and emits
 * {@code VALIDATION_FAILED}.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class BuyFromPrivateStoreCommand implements NxCommand<BuyFromPrivateStoreResult> {

    /** Upper bound the host clamps {@link #getTax() tax} to. */
    public static final int MAX_TAX_PERCENT = 50;

    /**
     * Hard cap on {@link #getLines() lines} — the host delivers the purchase as
     * a single mail with one attachment slot per line, and the engine's mail
     * attachment cap ({@code Config.MAIL_MAX_ATTACHMENTS}) is 36.
     */
    public static final int MAX_LINES = 36;

    private final int buyerCharId;
    private final int sellerCharId;
    private final List<BuyLine> lines;
    private final int tax;
    private final Instant deadline;

    public BuyFromPrivateStoreCommand(
            int buyerCharId, int sellerCharId, List<BuyLine> lines, int tax, Instant deadline) {
        if (buyerCharId <= 0) {
            throw new IllegalArgumentException("buyerCharId must be positive (got " + buyerCharId + ")");
        }
        if (sellerCharId <= 0) {
            throw new IllegalArgumentException("sellerCharId must be positive (got " + sellerCharId + ")");
        }
        if (buyerCharId == sellerCharId) {
            throw new IllegalArgumentException("buyerCharId must differ from sellerCharId (got " + buyerCharId + ")");
        }
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("lines is required and must be non-empty");
        }
        if (lines.size() > MAX_LINES) {
            throw new IllegalArgumentException(
                    "lines must not exceed MAX_LINES=" + MAX_LINES + " (got " + lines.size() + ")");
        }
        Set<Integer> seenItemIds = new HashSet<>();
        for (BuyLine line : lines) {
            if (line == null) {
                throw new IllegalArgumentException("lines must not contain null elements");
            }
            if (!seenItemIds.add(line.getItemId())) {
                throw new IllegalArgumentException("lines must not repeat itemId (duplicate " + line.getItemId() + ")");
            }
        }
        if (tax < 0 || tax > MAX_TAX_PERCENT) {
            throw new IllegalArgumentException("tax must be in 0.." + MAX_TAX_PERCENT + " (got " + tax + ")");
        }
        this.buyerCharId = buyerCharId;
        this.sellerCharId = sellerCharId;
        this.lines = PrivateStoreLists.freeze(lines);
        this.tax = tax;
        this.deadline = Objects.requireNonNull(deadline, "deadline");
    }

    /**
     * Character paying for the goods. Need not be online — the host loads an
     * offline character for the duration of the deal.
     */
    public int getBuyerCharId() {
        return buyerCharId;
    }

    /**
     * Character whose open sell-store is being bought from. MUST be in the
     * world (online or offline-trading) with a sell-store open.
     */
    public int getSellerCharId() {
        return sellerCharId;
    }

    /**
     * Lots to buy. REQUIRED, non-empty. Immutable on read.
     */
    public List<BuyLine> getLines() {
        return lines;
    }

    /**
     * Buyer-side surcharge in whole percent ({@code 5} = 5%) charged on top of
     * the lot price and burned — the seller receives the lot price only. The
     * host clamps to {@code 0..}{@link #MAX_TAX_PERCENT}. Fractional rates are
     * deliberately unsupported; {@code 0} means no surcharge.
     */
    public int getTax() {
        return tax;
    }

    /**
     * Moment after which the host MUST refuse to execute this command
     * ({@code COMMAND_EXPIRED}, nothing moves) instead of running it. The
     * platform stamps {@code now + reply-timeout} at dispatch; the host checks
     * this first, before resolving the seller or touching the seller's trade
     * list. Guards against a command sitting in the Kafka backlog (retention
     * ~3h) while the game-server was down and executing stale on restart.
     * REQUIRED — {@code null} rejected in the constructor.
     */
    public Instant getDeadline() {
        return deadline;
    }

    public Builder toBuilder() {
        return new Builder()
                .buyerCharId(buyerCharId)
                .sellerCharId(sellerCharId)
                .lines(lines)
                .tax(tax)
                .deadline(deadline);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BuyFromPrivateStoreCommand)) return false;
        BuyFromPrivateStoreCommand that = (BuyFromPrivateStoreCommand) o;
        return buyerCharId == that.buyerCharId
                && sellerCharId == that.sellerCharId
                && tax == that.tax
                && Objects.equals(lines, that.lines)
                && Objects.equals(deadline, that.deadline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buyerCharId, sellerCharId, lines, tax, deadline);
    }

    @Override
    public String toString() {
        return "BuyFromPrivateStoreCommand[buyerCharId=" + buyerCharId
                + ", sellerCharId=" + sellerCharId
                + ", lines=" + lines
                + ", tax=" + tax
                + ", deadline=" + deadline + "]";
    }

    public static final class Builder {
        private int buyerCharId;
        private int sellerCharId;
        private List<BuyLine> lines;
        private int tax;
        private Instant deadline;

        public Builder buyerCharId(int buyerCharId) {
            this.buyerCharId = buyerCharId;
            return this;
        }

        public Builder sellerCharId(int sellerCharId) {
            this.sellerCharId = sellerCharId;
            return this;
        }

        public Builder lines(List<BuyLine> lines) {
            this.lines = lines;
            return this;
        }

        public Builder tax(int tax) {
            this.tax = tax;
            return this;
        }

        public Builder deadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public BuyFromPrivateStoreCommand build() {
            return new BuyFromPrivateStoreCommand(buyerCharId, sellerCharId, lines, tax, deadline);
        }
    }
}
