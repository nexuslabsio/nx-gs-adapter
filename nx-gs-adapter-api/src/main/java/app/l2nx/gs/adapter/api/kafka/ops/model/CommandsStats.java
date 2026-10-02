package app.l2nx.gs.adapter.api.kafka.ops.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Heartbeat slot for the built-in {@code commands} module (inbound consumer + dispatch).
 * All counters are cumulative since adapter start.
 * <ul>
 *     <li>{@code otherServerSkippedTotal} - dropped for a non-matching or missing/malformed {@code Nx-Target-Server-Id}.</li>
 *     <li>{@code handledTotal} - handler returned a result (success or business error); excludes unsupported, validation and internal branches.</li>
 *     <li>{@code unsupportedTotal} - no {@code Nx-Message-Type} header or no registered handler.</li>
 *     <li>{@code validationFailedTotal} - payload deserialization failed.</li>
 *     <li>{@code internalErrorsTotal} - handler threw {@code RuntimeException}.</li>
 *     <li>{@code commitFailuresTotal} - offset commit failed; the record is redelivered on next poll.</li>
 *     <li>{@code deferredOpen} - gauge of deferred replies not yet completed.</li>
 *     <li>{@code deferredExpiredTotal} - deferred replies closed by the adapter because the host never completed them (host bug when rising).</li>
 * </ul>
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
    private final long deferredOpen;
    private final long deferredExpiredTotal;
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
            long deferredOpen,
            long deferredExpiredTotal,
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
        this.deferredOpen = deferredOpen;
        this.deferredExpiredTotal = deferredExpiredTotal;
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

    public long getDeferredOpen() {
        return deferredOpen;
    }

    public long getDeferredExpiredTotal() {
        return deferredExpiredTotal;
    }

    /** Registered command class simple names at heartbeat time; empty when none. */
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
                .deferredOpen(deferredOpen)
                .deferredExpiredTotal(deferredExpiredTotal)
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
                && deferredOpen == that.deferredOpen
                && deferredExpiredTotal == that.deferredExpiredTotal
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
                deferredOpen,
                deferredExpiredTotal,
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
                + ", deferredOpen=" + deferredOpen
                + ", deferredExpired=" + deferredExpiredTotal
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
        private long deferredOpen;
        private long deferredExpiredTotal;
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

        public Builder deferredOpen(long v) {
            this.deferredOpen = v;
            return this;
        }

        public Builder deferredExpiredTotal(long v) {
            this.deferredExpiredTotal = v;
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
                    deferredOpen,
                    deferredExpiredTotal,
                    registeredTypes);
        }
    }
}
