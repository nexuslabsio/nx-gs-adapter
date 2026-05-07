package app.l2nx.gs.adapter.api.kafka.commands;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Reply envelope for an inbound {@link NxCommand} dispatched through the
 * adapter's command runtime. Travels on {@code <tenant>.gs.commands.replies}
 * with header {@code Nx-Correlation-Id} echoed from the inbound command and
 * {@code Nx-Message-Type = "<OriginalCommandClass>Result"}.
 *
 * <p>Replaces the legacy free-form {@code ResponseV2.message: String} with a
 * structured discriminator: {@code success} flag + typed {@link ErrorCode} +
 * optional {@code errorDetails} string-string map for context (e.g.
 * {@code {"charId": "12345"}}, {@code {"error.class": "IllegalStateException"}})
 * + optional typed {@code payload} for success cases.</p>
 *
 * <p>Static factories cover the common shapes:</p>
 * <pre>
 *   CommandResult.&lt;Void&gt;success();
 *   CommandResult.success(payload);
 *   CommandResult.&lt;Void&gt;error(ErrorCode.NOT_FOUND);
 *   CommandResult.&lt;Void&gt;error(ErrorCode.NOT_FOUND, "charId", "12345");
 * </pre>
 *
 * <p>For multi-detail errors, use the fluent builder:</p>
 * <pre>
 *   CommandResult.&lt;Void&gt;builder()
 *           .errorCode(ErrorCode.VALIDATION_FAILED)
 *           .errorDetail("field", "amount")
 *           .errorDetail("got", "-100")
 *           .build();
 * </pre>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters} preserved constructor parameter names.</p>
 *
 * @param <R> success-payload type; {@code Void} for void replies.
 */
public final class CommandResult<R> {

    private final boolean success;
    private final @Nullable ErrorCode errorCode;
    private final Map<String, String> errorDetails;
    private final @Nullable R payload;

    public CommandResult(boolean success,
                         @Nullable ErrorCode errorCode,
                         @Nullable Map<String, String> errorDetails,
                         @Nullable R payload) {
        if (success && errorCode != null) {
            throw new IllegalArgumentException(
                    "success=true is mutually exclusive with errorCode (got " + errorCode + ")");
        }
        if (!success && errorCode == null) {
            throw new IllegalArgumentException(
                    "success=false requires a non-null errorCode");
        }
        this.success = success;
        this.errorCode = errorCode;
        this.errorDetails = freeze(errorDetails);
        this.payload = payload;
    }

    /**
     * Whether the command was processed successfully. {@code false} implies
     * {@link #getErrorCode()} is non-null.
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Standardized error category. Non-null iff {@link #isSuccess()} is
     * {@code false}.
     */
    public @Nullable ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Optional structured error context. Always non-null — {@code null} on
     * the wire is normalized to an empty map.
     */
    public Map<String, String> getErrorDetails() {
        return errorDetails;
    }

    /**
     * Optional success payload; {@code null} for void replies.
     */
    public @Nullable R getPayload() {
        return payload;
    }

    /**
     * Returns a new builder pre-populated with this instance's fields.
     * The success flag is implied by {@code errorCode == null}.
     */
    public Builder<R> toBuilder() {
        return new Builder<R>()
                .errorCode(errorCode)
                .errorDetails(errorDetails)
                .payload(payload);
    }

    public static <R> Builder<R> builder() {
        return new Builder<R>();
    }

    /**
     * Success reply with no payload.
     */
    public static <R> CommandResult<R> success() {
        return new CommandResult<R>(true, null, null, null);
    }

    /**
     * Success reply carrying a typed payload.
     */
    public static <R> CommandResult<R> success(@Nullable R payload) {
        return new CommandResult<R>(true, null, null, payload);
    }

    /**
     * Error reply with no extra context.
     */
    public static <R> CommandResult<R> error(ErrorCode code) {
        return new CommandResult<R>(false, code, null, null);
    }

    /**
     * Error reply with a single key-value detail.
     */
    public static <R> CommandResult<R> error(ErrorCode code, String key, String value) {
        Map<String, String> details = new LinkedHashMap<String, String>();
        details.put(key, value);
        return new CommandResult<R>(false, code, details, null);
    }

    private static Map<String, String> freeze(@Nullable Map<String, String> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, String>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommandResult)) return false;
        CommandResult<?> that = (CommandResult<?>) o;
        return success == that.success
                && Objects.equals(errorCode, that.errorCode)
                && Objects.equals(errorDetails, that.errorDetails)
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, errorCode, errorDetails, payload);
    }

    @Override
    public String toString() {
        return "CommandResult[success=" + success
                + ", errorCode=" + errorCode
                + ", errorDetails=" + errorDetails
                + ", payload=" + payload + "]";
    }

    /**
     * Builder enforcing the {@link CommandResult} invariant: the {@code success}
     * flag is implied by {@code errorCode == null}, never set independently.
     * Static factories ({@link CommandResult#success()}, {@link
     * CommandResult#error(ErrorCode)}, etc.) cover the common shapes; this
     * builder is for multi-detail error replies.
     */
    public static final class Builder<R> {

        private @Nullable ErrorCode errorCode;
        private @Nullable Map<String, String> errorDetails;
        private @Nullable R payload;

        /**
         * Set the error code. {@code null} produces a success reply on
         * {@link #build()}; non-null produces an error reply.
         */
        public Builder<R> errorCode(@Nullable ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder<R> errorDetails(@Nullable Map<String, String> errorDetails) {
            this.errorDetails = errorDetails;
            return this;
        }

        /**
         * Append a single key-value detail to the builder. Initializes the
         * map on first call; if a frozen map was previously set via
         * {@link #errorDetails(Map)} or {@link CommandResult#toBuilder()},
         * defensively copies it into a fresh mutable {@link LinkedHashMap}
         * before mutating. Last write wins on duplicate keys.
         */
        public Builder<R> errorDetail(String key, String value) {
            if (errorDetails == null) {
                errorDetails = new LinkedHashMap<String, String>();
            } else if (!(errorDetails instanceof LinkedHashMap)) {
                errorDetails = new LinkedHashMap<String, String>(errorDetails);
            }
            errorDetails.put(key, value);
            return this;
        }

        public Builder<R> payload(@Nullable R payload) {
            this.payload = payload;
            return this;
        }

        public CommandResult<R> build() {
            return new CommandResult<R>(errorCode == null, errorCode, errorDetails, payload);
        }
    }
}
