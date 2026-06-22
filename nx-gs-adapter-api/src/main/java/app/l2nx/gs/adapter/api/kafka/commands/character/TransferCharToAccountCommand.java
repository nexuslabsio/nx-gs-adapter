package app.l2nx.gs.adapter.api.kafka.commands.character;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;

/**
 * Inbound command instructing the game-server to move a character from its
 * current login account to a different one. The character itself stays
 * intact (same {@code charId}, items, clan, progression) — only the
 * underlying {@code account_name} pointer is rewritten so the next login
 * succeeds from the new account's credentials.
 *
 * <p>Reply: {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <Void>}
 * — {@code success()} on a successful account rebind; common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — character does not exist on this server.</li>
 *     <li>{@code INVALID_STATE} — character cannot be rebound right now
 *     (logged in to a non-disconnectable state, jailed, in olympiad, in
 *     siege, …); the host's policy defines the exact rejection set.</li>
 *     <li>{@code FORBIDDEN} — operation rejected on policy grounds (e.g.
 *     moving a banned character is disallowed by the host's audit policy).</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing a required field
 *     (Gson defaults boxed {@code Long} / nullable {@code String} to
 *     {@code null} on missing wire field; handler MUST check non-null
 *     before applying).</li>
 *     <li>{@code UNAVAILABLE} — transient persistence failure (DB
 *     unreachable, contention timeout on the {@code characters} row
 *     lock); retry may succeed.</li>
 * </ul>
 *
 * <p><b>Side effects.</b> If the character is logged in at the time of
 * the command, the handler SHOULD force a logout before the rebind so the
 * client does not observe inconsistent account state mid-session.</p>
 *
 * <p><b>Required fields.</b> Both fields ({@code charId},
 * {@code accountTo}) are semantically REQUIRED. The constructor enforces
 * non-null via {@link IllegalArgumentException} for programmatic
 * construction. Wire-path Gson bypasses the constructor — handler-side
 * null-checking is the wire-validation gate.</p>
 *
 * <p><b>Partitioning.</b> Routed by {@link #getCharId() charId} on the
 * commands topic — sequential with other character-scoped operations on
 * the same character.</p>
 *
 * <p><b>Idempotency.</b> Handler MUST be idempotent — Kafka redelivery on
 * crash recovery may re-invoke the handler with the same
 * {@code Nx-Correlation-Id}. If the character's {@code account_name}
 * already matches {@link #getAccountTo() accountTo}, the handler SHOULD
 * treat the call as a no-op success rather than re-issuing the UPDATE.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class TransferCharToAccountCommand implements NxCommand<TransferCharToAccountResult> {

    private final Long charId;
    private final String accountTo;

    public TransferCharToAccountCommand(Long charId, String accountTo) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (accountTo == null) {
            throw new IllegalArgumentException("accountTo is required");
        }
        this.charId = charId;
        this.accountTo = accountTo;
    }

    /**
     * Character primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field
     * (boxed {@code Long} surfaces missing wire data as {@code null}).
     */
    public Long getCharId() {
        return charId;
    }

    /**
     * Target login-account name. REQUIRED, non-blank semantically.
     * Free-form host-supplied identifier — the value MUST exist as an
     * account on the login server (handler MAY verify and reply
     * {@code NOT_FOUND} when missing, or leave verification to the next
     * login attempt depending on host policy).
     */
    public String getAccountTo() {
        return accountTo;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).accountTo(accountTo);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransferCharToAccountCommand)) return false;
        TransferCharToAccountCommand that = (TransferCharToAccountCommand) o;
        return Objects.equals(charId, that.charId) && Objects.equals(accountTo, that.accountTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, accountTo);
    }

    @Override
    public String toString() {
        return "TransferCharToAccountCommand[charId=" + charId + ", accountTo=" + accountTo + "]";
    }

    public static final class Builder {
        private Long charId;
        private String accountTo;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder accountTo(String accountTo) {
            this.accountTo = accountTo;
            return this;
        }

        public TransferCharToAccountCommand build() {
            return new TransferCharToAccountCommand(charId, accountTo);
        }
    }
}
