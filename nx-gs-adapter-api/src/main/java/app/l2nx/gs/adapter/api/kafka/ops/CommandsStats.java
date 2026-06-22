package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Heartbeat slot reporting health of the built-in {@code commands} module —
 * the inbound Kafka consumer + dispatch table that drives
 * {@link app.l2nx.gs.adapter.api.spi.NxCommands} handler invocations.
 *
 * <p>Lives inside {@link ModuleStatus.Stats} alongside {@code pool} (for
 * DB-reading sync modules), {@code entities} (for per-entity sync progress),
 * and {@code events} (for outbound publisher health). Producer side:
 * {@code nx-gs-adapter-core}'s {@code CommandsConsumer.currentStatus()}.</p>
 *
 * <p>Counter taxonomy (cumulative since adapter start):</p>
 * <ul>
 *     <li>{@code consumed-total} — records pulled from Kafka.</li>
 *     <li>{@code other-server-skipped-total} — records dropped because the
 *     {@code Nx-Target-Server-Id} header did not match this adapter's own
 *     server id (cross-server multiplexing on the shared per-tenant commands
 *     topic) or the header was missing/malformed.</li>
 *     <li>{@code handled-total} — records dispatched to a handler that returned
 *     a {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult} (success
 *     OR business error). Excludes records that hit
 *     {@code unsupported / validation / internal} branches.</li>
 *     <li>{@code unsupported-total} — records the dispatcher could not route:
 *     missing {@code Nx-Message-Type} header OR the header value has no
 *     registered handler. Reply emitted with
 *     {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#UNSUPPORTED_COMMAND}.</li>
 *     <li>{@code validation-failed-total} — Gson deserialization failures.
 *     Reply emitted with
 *     {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#VALIDATION_FAILED}.</li>
 *     <li>{@code internal-errors-total} — handler {@code RuntimeException}
 *     count. Reply emitted with
 *     {@link app.l2nx.gs.adapter.api.kafka.commands.CommandStatus#INTERNAL_ERROR}.</li>
 *     <li>{@code replies-published-total} — Kafka acks for reply records.</li>
 *     <li>{@code replies-failed-total} — Kafka send-callback errors on reply
 *     records.</li>
 *     <li>{@code commit-failures-total} — manual offset commit errors. Records
 *     stay uncommitted; redelivery on next poll.</li>
 * </ul>
 *
 * <p>{@code registered-types} is a snapshot list of registered command class
 * simple names — debugging aid for "why is this command going UNSUPPORTED?"
 * checks. Static across runtime once {@code onConnect} completes.</p>
 */
public final class CommandsStats {

    private final long consumedTotal;
    private final long otherServerSkippedTotal;
    private final long handledTotal;
    private final long unsupportedTotal;
    private final long validationFailedTotal;
    private final long internalErrorsTotal;
    private final long repliesPublishedTotal;
    private final long repliesFailedTotal;
    private final long commitFailuresTotal;
    private final @Nullable List<String> registeredTypes;

    public CommandsStats(
            long consumedTotal,
            long otherServerSkippedTotal,
            long handledTotal,
            long unsupportedTotal,
            long validationFailedTotal,
            long internalErrorsTotal,
            long repliesPublishedTotal,
            long repliesFailedTotal,
            long commitFailuresTotal,
            @Nullable List<String> registeredTypes) {
        this.consumedTotal = consumedTotal;
        this.otherServerSkippedTotal = otherServerSkippedTotal;
        this.handledTotal = handledTotal;
        this.unsupportedTotal = unsupportedTotal;
        this.validationFailedTotal = validationFailedTotal;
        this.internalErrorsTotal = internalErrorsTotal;
        this.repliesPublishedTotal = repliesPublishedTotal;
        this.repliesFailedTotal = repliesFailedTotal;
        this.commitFailuresTotal = commitFailuresTotal;
        this.registeredTypes = freeze(registeredTypes);
    }

    public long getConsumedTotal() {
        return consumedTotal;
    }

    public long getOtherServerSkippedTotal() {
        return otherServerSkippedTotal;
    }

    public long getHandledTotal() {
        return handledTotal;
    }

    public long getUnsupportedTotal() {
        return unsupportedTotal;
    }

    public long getValidationFailedTotal() {
        return validationFailedTotal;
    }

    public long getInternalErrorsTotal() {
        return internalErrorsTotal;
    }

    public long getRepliesPublishedTotal() {
        return repliesPublishedTotal;
    }

    public long getRepliesFailedTotal() {
        return repliesFailedTotal;
    }

    public long getCommitFailuresTotal() {
        return commitFailuresTotal;
    }

    /**
     * Snapshot of {@code Nx-Message-Type} class simple names registered with
     * {@code NxCommands.on(...)} at heartbeat tick time. Empty when none.
     */
    public List<String> getRegisteredTypes() {
        return registeredTypes == null ? Collections.emptyList() : registeredTypes;
    }

    public Builder toBuilder() {
        return new Builder()
                .consumedTotal(consumedTotal)
                .otherServerSkippedTotal(otherServerSkippedTotal)
                .handledTotal(handledTotal)
                .unsupportedTotal(unsupportedTotal)
                .validationFailedTotal(validationFailedTotal)
                .internalErrorsTotal(internalErrorsTotal)
                .repliesPublishedTotal(repliesPublishedTotal)
                .repliesFailedTotal(repliesFailedTotal)
                .commitFailuresTotal(commitFailuresTotal)
                .registeredTypes(registeredTypes);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static @Nullable List<String> freeze(@Nullable List<String> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableList(new ArrayList<String>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommandsStats)) return false;
        CommandsStats that = (CommandsStats) o;
        return consumedTotal == that.consumedTotal
                && otherServerSkippedTotal == that.otherServerSkippedTotal
                && handledTotal == that.handledTotal
                && unsupportedTotal == that.unsupportedTotal
                && validationFailedTotal == that.validationFailedTotal
                && internalErrorsTotal == that.internalErrorsTotal
                && repliesPublishedTotal == that.repliesPublishedTotal
                && repliesFailedTotal == that.repliesFailedTotal
                && commitFailuresTotal == that.commitFailuresTotal
                && Objects.equals(registeredTypes, that.registeredTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                consumedTotal,
                otherServerSkippedTotal,
                handledTotal,
                unsupportedTotal,
                validationFailedTotal,
                internalErrorsTotal,
                repliesPublishedTotal,
                repliesFailedTotal,
                commitFailuresTotal,
                registeredTypes);
    }

    @Override
    public String toString() {
        return "CommandsStats[consumed=" + consumedTotal
                + ", otherServerSkipped=" + otherServerSkippedTotal
                + ", handled=" + handledTotal
                + ", unsupported=" + unsupportedTotal
                + ", validationFailed=" + validationFailedTotal
                + ", internalErrors=" + internalErrorsTotal
                + ", repliesPublished=" + repliesPublishedTotal
                + ", repliesFailed=" + repliesFailedTotal
                + ", commitFailures=" + commitFailuresTotal
                + ", registeredTypes=" + registeredTypes + "]";
    }

    public static final class Builder {
        private long consumedTotal;
        private long otherServerSkippedTotal;
        private long handledTotal;
        private long unsupportedTotal;
        private long validationFailedTotal;
        private long internalErrorsTotal;
        private long repliesPublishedTotal;
        private long repliesFailedTotal;
        private long commitFailuresTotal;
        private @Nullable List<String> registeredTypes;

        public Builder consumedTotal(long v) {
            this.consumedTotal = v;
            return this;
        }

        public Builder otherServerSkippedTotal(long v) {
            this.otherServerSkippedTotal = v;
            return this;
        }

        public Builder handledTotal(long v) {
            this.handledTotal = v;
            return this;
        }

        public Builder unsupportedTotal(long v) {
            this.unsupportedTotal = v;
            return this;
        }

        public Builder validationFailedTotal(long v) {
            this.validationFailedTotal = v;
            return this;
        }

        public Builder internalErrorsTotal(long v) {
            this.internalErrorsTotal = v;
            return this;
        }

        public Builder repliesPublishedTotal(long v) {
            this.repliesPublishedTotal = v;
            return this;
        }

        public Builder repliesFailedTotal(long v) {
            this.repliesFailedTotal = v;
            return this;
        }

        public Builder commitFailuresTotal(long v) {
            this.commitFailuresTotal = v;
            return this;
        }

        public Builder registeredTypes(@Nullable List<String> registeredTypes) {
            this.registeredTypes = registeredTypes;
            return this;
        }

        public CommandsStats build() {
            return new CommandsStats(
                    consumedTotal,
                    otherServerSkippedTotal,
                    handledTotal,
                    unsupportedTotal,
                    validationFailedTotal,
                    internalErrorsTotal,
                    repliesPublishedTotal,
                    repliesFailedTotal,
                    commitFailuresTotal,
                    registeredTypes);
        }
    }
}
