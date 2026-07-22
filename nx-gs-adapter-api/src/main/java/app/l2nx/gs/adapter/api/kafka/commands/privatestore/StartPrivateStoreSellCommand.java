package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Inbound command instructing the game-server to open a regular
 * ("sell one by one") private store on behalf of a character, listing the
 * given inventory stacks at their asked prices. Executed by the host's
 * private-store subsystem on the character's game thread.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link StartPrivateStoreResult}{@code >}
 * — {@code success(payload)} carries the accepted-line count plus any lines
 * the host rejected (see {@link StartPrivateStoreResult}). Common error
 * replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — {@code charId} does not exist / is not online
 *     on this server.</li>
 *     <li>{@code VALIDATION_FAILED} — {@code lines} missing/empty, or any
 *     {@link SellLine} entry is malformed.</li>
 *     <li>{@code INVALID_STATE} — the character cannot open a store right
 *     now (in combat, already trading, dead, …).</li>
 * </ul>
 *
 * <p><b>Required fields.</b> {@link #getCharId() charId} and a non-empty
 * {@link #getLines() lines} are REQUIRED — the constructor enforces this via
 * {@link IllegalArgumentException} for programmatic construction. Wire-path
 * deserialization bypasses the constructor — the handler re-checks and emits
 * {@code VALIDATION_FAILED}. {@link #getTitle() title} is OPTIONAL —
 * {@code null} falls back to the host's default store banner text.</p>
 *
 * <p><b>Partial acceptance.</b> The host MAY reject individual lines (item no
 * longer in inventory, not tradeable, …) while still opening the store with
 * the remaining lines; rejected lines are reported in
 * {@link StartPrivateStoreResult#getDropped() dropped} on the success
 * envelope. Only {@code VALIDATION_FAILED} on the whole command halts the
 * store from opening at all.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class StartPrivateStoreSellCommand implements NxCommand<StartPrivateStoreResult> {

    private final int charId;
    private final @Nullable String title;
    private final List<SellLine> lines;

    public StartPrivateStoreSellCommand(int charId, @Nullable String title, List<SellLine> lines) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("lines is required and must be non-empty");
        }
        this.charId = charId;
        this.title = title;
        this.lines = PrivateStoreLists.freeze(lines);
    }

    public int getCharId() {
        return charId;
    }

    /**
     * Store banner text shown above the seller. OPTIONAL — {@code null}
     * falls back to the host's default.
     */
    public @Nullable String getTitle() {
        return title;
    }

    /**
     * Offered stacks. REQUIRED, non-empty. Immutable on read.
     */
    public List<SellLine> getLines() {
        return lines;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).title(title).lines(lines);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StartPrivateStoreSellCommand)) return false;
        StartPrivateStoreSellCommand that = (StartPrivateStoreSellCommand) o;
        return charId == that.charId && Objects.equals(title, that.title) && Objects.equals(lines, that.lines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, title, lines);
    }

    @Override
    public String toString() {
        return "StartPrivateStoreSellCommand[charId=" + charId + ", title=" + title + ", lines=" + lines + "]";
    }

    public static final class Builder {
        private int charId;
        private @Nullable String title;
        private @Nullable List<SellLine> lines;

        public Builder charId(int charId) {
            this.charId = charId;
            return this;
        }

        public Builder title(@Nullable String title) {
            this.title = title;
            return this;
        }

        public Builder lines(List<SellLine> lines) {
            this.lines = lines;
            return this;
        }

        public StartPrivateStoreSellCommand build() {
            return new StartPrivateStoreSellCommand(charId, title, lines);
        }
    }
}
