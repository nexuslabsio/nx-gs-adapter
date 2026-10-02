package app.l2nx.gs.adapter.api.kafka.commands;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Reply envelope for an inbound {@link NxCommand}, sent on {@code <tenant>.gs.commands.replies} with
 * {@code Nx-Correlation-Id} echoed and {@code Nx-Message-Type = "<CommandNameWithoutCommandSuffix>Result"}.
 *
 * <p>Invariant: payload is non-null iff status is OK; problem is non-null iff status is not OK. The constructor
 * enforces it, but Gson bypasses the constructor, so consumers should assume it when reading.</p>
 *
 * <p>Success data belongs in the {@code R} payload, never in problem extensions (failure context only).</p>
 *
 * @param <R> success-payload type declared on {@link NxCommand}; {@link Void} when there is none
 */
public final class CommandResult<R> {

    private final CommandStatus status;
    private final @Nullable R payload;
    private final @Nullable CommandProblem problem;

    public CommandResult(CommandStatus status, @Nullable R payload, @Nullable CommandProblem problem) {
        if (status == null) {
            throw new IllegalArgumentException("status is required");
        }
        if (status == CommandStatus.OK && problem != null) {
            throw new IllegalArgumentException("status=OK is mutually exclusive with problem (got " + problem + ")");
        }
        if (status != CommandStatus.OK && problem == null) {
            throw new IllegalArgumentException("status=" + status + " requires a non-null problem");
        }
        if (status != CommandStatus.OK && payload != null) {
            throw new IllegalArgumentException(
                    "status=" + status + " is mutually exclusive with payload (got " + payload + ")");
        }
        this.status = status;
        this.payload = payload;
        this.problem = problem;
    }

    public CommandStatus getStatus() {
        return status;
    }

    public CommandStatus.Tier getTier() {
        return status.tier();
    }

    public boolean isOk() {
        return status == CommandStatus.OK;
    }

    public @Nullable R getPayload() {
        return payload;
    }

    public @Nullable CommandProblem getProblem() {
        return problem;
    }

    public static <R> CommandResult<R> ok() {
        return new CommandResult<R>(CommandStatus.OK, null, null);
    }

    public static <R> CommandResult<R> ok(R payload) {
        return new CommandResult<R>(CommandStatus.OK, payload, null);
    }

    public static <R> CommandResult<R> error(CommandStatus status, CommandProblem problem) {
        return new CommandResult<R>(status, null, problem);
    }

    public static <R> CommandResult<R> error(CommandStatus status, String title) {
        return new CommandResult<R>(status, null, CommandProblem.of(title));
    }

    public static <R> CommandResult<R> error(CommandStatus status, String title, String extKey, Object extValue) {
        return new CommandResult<R>(status, null, CommandProblem.of(title, extKey, extValue));
    }

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

    public static <R> CommandResult<R> commandExpired(String title) {
        return error(CommandStatus.COMMAND_EXPIRED, title);
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
        return status == that.status && Objects.equals(payload, that.payload) && Objects.equals(problem, that.problem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, payload, problem);
    }

    @Override
    public String toString() {
        return "CommandResult[status=" + status + ", payload=" + payload + ", problem=" + problem + "]";
    }
}
