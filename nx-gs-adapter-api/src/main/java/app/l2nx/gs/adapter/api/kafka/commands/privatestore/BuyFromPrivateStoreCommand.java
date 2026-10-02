package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.BuyLine;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Buys lots from another character's open sell-store on behalf of {@code buyerCharId} (remote "buy now"); the buyer
 * need not be online, in range, or in the same world instance as the seller.
 *
 * <p>{@code VALIDATION_FAILED}: malformed lines, blank mail text, buyer equals seller. {@code NOT_FOUND}: seller absent
 * or no open sell-store. {@code INVALID_STATE}: lot changed, store type not served, buyer cannot receive the goods.
 * {@code FORBIDDEN}: buyer barred from trading. {@code COMMAND_EXPIRED}: deadline passed, nothing moved.
 * Gson bypasses the constructor, so the handler re-checks.</p>
 *
 * <p>Every non-OK reply carries a stable {@code reason} code in
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandProblem#getExtensions() CommandProblem.extensions}, with numeric
 * context in sibling keys. The platform localizes; the host sends no player-facing text and writes the
 * already-localized mail text verbatim.</p>
 *
 * <p>All-or-nothing: every line is bought at exactly the requested count and price, or nothing is charged or moved.
 * The host validates all lots against the seller's live trade list before entering the engine, so a stale order book
 * fails instead of silently buying less.</p>
 */
public final class BuyFromPrivateStoreCommand implements NxCommand<BuyFromPrivateStoreResult> {

    public static final int MAX_TAX_PERCENT = 50;

    /**
     * Delivery is one mail with a slot per line; the engine's attachment cap ({@code Config.MAIL_MAX_ATTACHMENTS}) is 36.
     */
    public static final int MAX_LINES = 36;

    private final int buyerCharId;
    private final int sellerCharId;
    private final List<BuyLine> lines;
    private final int tax;
    private final Instant deadline;
    private final String mailSender;
    private final String mailSubject;
    private final String mailBody;

    public BuyFromPrivateStoreCommand(
            int buyerCharId,
            int sellerCharId,
            List<BuyLine> lines,
            int tax,
            Instant deadline,
            String mailSender,
            String mailSubject,
            String mailBody) {
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
        this.mailSender = requireText(mailSender, "mailSender");
        this.mailSubject = requireText(mailSubject, "mailSubject");
        this.mailBody = requireText(mailBody, "mailBody");
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    /** Need not be online; the host loads an offline character for the deal. */
    public int getBuyerCharId() {
        return buyerCharId;
    }

    /** Must be in the world (online or offline-trading) with a sell-store open. */
    public int getSellerCharId() {
        return sellerCharId;
    }

    public List<BuyLine> getLines() {
        return lines;
    }

    /**
     * Whole percent charged on top of the lot price and burned; the seller gets the lot price only. The host clamps to
     * {@code 0..}{@link #MAX_TAX_PERCENT}.
     */
    public int getTax() {
        return tax;
    }

    /**
     * The host MUST refuse after this ({@code COMMAND_EXPIRED}, nothing moves), checked before touching the seller's trade
     * list. Guards against executing stale after sitting in the Kafka backlog (retention ~3h) while the game-server was down.
     */
    public Instant getDeadline() {
        return deadline;
    }

    /** Final, already-localized text with no placeholders; no locale travels on the wire. */
    public String getMailSender() {
        return mailSender;
    }

    public String getMailSubject() {
        return mailSubject;
    }

    public String getMailBody() {
        return mailBody;
    }

    public Builder toBuilder() {
        return new Builder()
                .buyerCharId(buyerCharId)
                .sellerCharId(sellerCharId)
                .lines(lines)
                .tax(tax)
                .deadline(deadline)
                .mailSender(mailSender)
                .mailSubject(mailSubject)
                .mailBody(mailBody);
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
                && Objects.equals(deadline, that.deadline)
                && Objects.equals(mailSender, that.mailSender)
                && Objects.equals(mailSubject, that.mailSubject)
                && Objects.equals(mailBody, that.mailBody);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buyerCharId, sellerCharId, lines, tax, deadline, mailSender, mailSubject, mailBody);
    }

    @Override
    public String toString() {
        return "BuyFromPrivateStoreCommand[buyerCharId=" + buyerCharId
                + ", sellerCharId=" + sellerCharId
                + ", lines=" + lines
                + ", tax=" + tax
                + ", deadline=" + deadline
                + ", mailSender=" + mailSender
                + ", mailSubject=" + mailSubject
                + ", mailBody=" + mailBody + "]";
    }

    public static final class Builder {
        private int buyerCharId;
        private int sellerCharId;
        private List<BuyLine> lines;
        private int tax;
        private Instant deadline;
        private String mailSender;
        private String mailSubject;
        private String mailBody;

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

        public Builder mailSender(String mailSender) {
            this.mailSender = mailSender;
            return this;
        }

        public Builder mailSubject(String mailSubject) {
            this.mailSubject = mailSubject;
            return this;
        }

        public Builder mailBody(String mailBody) {
            this.mailBody = mailBody;
            return this;
        }

        public BuyFromPrivateStoreCommand build() {
            return new BuyFromPrivateStoreCommand(
                    buyerCharId, sellerCharId, lines, tax, deadline, mailSender, mailSubject, mailBody);
        }
    }
}
