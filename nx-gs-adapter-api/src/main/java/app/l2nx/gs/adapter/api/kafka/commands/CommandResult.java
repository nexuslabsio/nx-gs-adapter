package app.l2nx.gs.adapter.api.kafka.commands;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Reply envelope for an inbound {@link NxCommand}. Travels on
 * {@code <tenant>.gs.commands.replies} with header {@code Nx-Correlation-Id}
 * echoed from the inbound command and {@code Nx-Message-Type =
 * "<OriginalCommandClassNameWithoutCommandSuffix>Result"} (e.g.
 * {@code "TransferItemResult"}).
 *
 * <p><b>Invariant.</b> {@link #getPayload() payload} is non-null iff
 * {@link #getStatus() status} is {@link CommandStatus#OK}; {@link #getProblem()
 * problem} is non-null iff status is NOT OK. The constructor enforces this
 * for programmatic construction. Wire-path Gson bypasses the constructor —
 * platform consumers SHOULD assume the invariant when reading.</p>
 *
 * <p>Common shapes:</p>
 * <pre>
 *   CommandResult.&lt;DeleteItemResult&gt;ok(new DeleteItemResult(...));
 *   CommandResult.&lt;Void&gt;ok();                                    // marker-only success
 *   CommandResult.&lt;Void&gt;notFound("Character not found");
 *   CommandResult.&lt;Void&gt;notFound("Character not found", "charId", 12345L);
 *   CommandResult.&lt;Void&gt;validationFailed("count must be positive", "field", "count");
 *   CommandResult.&lt;Void&gt;error(CommandStatus.FORBIDDEN,
 *           CommandProblem.of("Self-punishment not allowed"));
 * </pre>
 *
 * <p>Domain-specific success data (partial-success flags, affected entity
 * ids, modes) lives in the {@code R} payload class — NOT in
 * {@link CommandProblem#getExtensions() problem.extensions}, which is
 * reserved for failure context.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 *
 * @param <R> success-payload type carried by this command (declared on
 *            {@link NxCommand}{@code <R>}); use {@link Void} for commands
 *            whose OK reply carries no typed data.
 */
public final class CommandResult<R> {

    private final CommandStatus status;
    private final @Nullable R payload;
    private final @Nullable CommandProblem problem;

    public CommandResult(CommandStatus status,
                         @Nullable R payload,
                         @Nullable CommandProblem problem) {
        if (status == null) {
            throw new IllegalArgumentException("status is required");
        }
        if (status == CommandStatus.OK && problem != null) {
            throw new IllegalArgumentException(
                    "status=OK is mutually exclusive with problem (got " + problem + ")");
        }
        if (status != CommandStatus.OK && problem == null) {
            throw new IllegalArgumentException(
                    "status=" + status + " requires a non-null problem");
        }
        if (status != CommandStatus.OK && payload != null) {
            throw new IllegalArgumentException(
                    "status=" + status + " is mutually exclusive with payload (got "
                            + payload + ")");
        }
        this.status = status;
        this.payload = payload;
        this.problem = problem;
    }

    public CommandStatus getStatus() {
        return status;
    }

    /**
     * Coarse 3-way classification (OK / CLIENT_ERROR / SERVER_ERROR).
     * Shorthand for {@code getStatus().tier()}.
     */
    public CommandStatus.Tier getTier() {
        return status.tier();
    }

    public boolean isOk() {
        return status == CommandStatus.OK;
    }

    /**
     * Success payload; non-null iff {@link #isOk()}.
     */
    public @Nullable R getPayload() {
        return payload;
    }

    /**
     * Failure context; non-null iff NOT {@link #isOk()}.
     */
    public @Nullable CommandProblem getProblem() {
        return problem;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Static factories
    // ─────────────────────────────────────────────────────────────────────

    /**
     * OK reply with no typed payload (use when {@code R == Void}).
     */
    public static <R> CommandResult<R> ok() {
        return new CommandResult<R>(CommandStatus.OK, null, null);
    }

    /**
     * OK reply with a typed payload.
     */
    public static <R> CommandResult<R> ok(R payload) {
        return new CommandResult<R>(CommandStatus.OK, payload, null);
    }

    /**
     * Error reply with a pre-built {@link CommandProblem}.
     */
    public static <R> CommandResult<R> error(CommandStatus status, CommandProblem problem) {
        return new CommandResult<R>(status, null, problem);
    }

    /**
     * Error reply with just a title; the problem body has no extensions.
     */
    public static <R> CommandResult<R> error(CommandStatus status, String title) {
        return new CommandResult<R>(status, null, CommandProblem.of(title));
    }

    /**
     * Error reply with title + single-key extension context.
     */
    public static <R> CommandResult<R> error(CommandStatus status,
                                             String title,
                                             String extKey,
                                             Object extValue) {
        return new CommandResult<R>(status, null, CommandProblem.of(title, extKey, extValue));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Sugar: per-status factories for the common cases
    // ─────────────────────────────────────────────────────────────────────

    public static <R> CommandResult<R> notFound(String title) {
        return error(CommandStatus.NOT_FOUND, title);
    }

    public static <R> CommandResult<R> notFound(String title, String extKey, Object extValue) {
        return error(CommandStatus.NOT_FOUND, title, extKey, extValue);
    }

    public static <R> CommandResult<R> invalidState(String title) {
        return error(CommandStatus.INVALID_STATE, title);
    }

    public static <R> CommandResult<R> invalidState(String title, String extKey, Object extValue) {
        return error(CommandStatus.INVALID_STATE, title, extKey, extValue);
    }

    public static <R> CommandResult<R> forbidden(String title) {
        return error(CommandStatus.FORBIDDEN, title);
    }

    public static <R> CommandResult<R> validationFailed(String title) {
        return error(CommandStatus.VALIDATION_FAILED, title);
    }

    public static <R> CommandResult<R> validationFailed(String title, String field) {
        return error(CommandStatus.VALIDATION_FAILED, title, "field", field);
    }

    public static <R> CommandResult<R> rateLimited(String title) {
        return error(CommandStatus.RATE_LIMITED, title);
    }

    public static <R> CommandResult<R> unavailable(String title) {
        return error(CommandStatus.UNAVAILABLE, title);
    }

    public static <R> CommandResult<R> internalError(String title) {
        return error(CommandStatus.INTERNAL_ERROR, title);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommandResult)) return false;
        CommandResult<?> that = (CommandResult<?>) o;
        return status == that.status
                && Objects.equals(payload, that.payload)
                && Objects.equals(problem, that.problem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, payload, problem);
    }

    @Override
    public String toString() {
        return "CommandResult[status=" + status
                + ", payload=" + payload
                + ", problem=" + problem + "]";
    }
}
