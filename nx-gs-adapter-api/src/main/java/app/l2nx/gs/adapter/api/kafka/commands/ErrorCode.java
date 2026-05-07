package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * Standardized error categories for {@link CommandResult}. Replaces the legacy
 * free-form {@code message: String} field with a discriminator the platform
 * side can switch on for retry / alert routing without parsing strings.
 *
 * <p>Wire form: enum constant name (uppercase). Consumers SHOULD treat unknown
 * values as a generic failure category for forward-compat when new codes ship.</p>
 *
 * <p>Picking the right code:</p>
 * <ul>
 *     <li>{@link #NOT_FOUND} — subject of the operation does not exist
 *     (character, account, item, mail).</li>
 *     <li>{@link #INVALID_STATE} — subject exists but cannot accept the
 *     operation in its current state (player in jail, item already sold).</li>
 *     <li>{@link #FORBIDDEN} — operation rejected on policy grounds regardless
 *     of state (cannot punish self, cannot kick admin).</li>
 *     <li>{@link #RATE_LIMITED} — handler self-rate-limited; retry later may
 *     succeed.</li>
 *     <li>{@link #UNAVAILABLE} — transient dependency failure (DB unreachable,
 *     downstream timeout); retry may succeed.</li>
 *     <li>{@link #VALIDATION_FAILED} — the command payload itself is malformed
 *     (negative quantity, missing required field, wrong shape).</li>
 *     <li>{@link #INTERNAL_ERROR} — unexpected error. Auto-emitted by the
 *     adapter when a handler throws {@code RuntimeException}; handlers MAY
 *     also emit it explicitly for "shouldn't happen" branches.</li>
 *     <li>{@link #UNSUPPORTED_COMMAND} — adapter-emitted only. Indicates the
 *     inbound {@code Nx-Message-Type} header has no registered handler. Web
 *     side either has not registered a handler yet or is sending a command the
 *     deployed core does not know about.</li>
 * </ul>
 */
public enum ErrorCode {

    /**
     * Subject of the command (char, account, item, …) does not exist.
     */
    NOT_FOUND,

    /**
     * Subject exists but cannot accept the operation right now.
     */
    INVALID_STATE,

    /**
     * Operation rejected on policy grounds regardless of state.
     */
    FORBIDDEN,

    /**
     * Handler self-rate-limited; retry after a delay may succeed.
     */
    RATE_LIMITED,

    /**
     * Transient dependency failure; retry may succeed.
     */
    UNAVAILABLE,

    /**
     * Command payload is malformed (validation, missing fields, wrong types).
     */
    VALIDATION_FAILED,

    /**
     * Unexpected error. Auto-emitted on handler {@code RuntimeException}.
     */
    INTERNAL_ERROR,

    /**
     * Adapter-emitted only — the inbound {@code Nx-Message-Type} header
     * resolved to no registered handler. Indicates a deployment skew between
     * platform and core.
     */
    UNSUPPORTED_COMMAND
}
