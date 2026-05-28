package app.l2nx.gs.adapter.api.kafka.events.account;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * Discrete account-authentication attempt fact — one event per credential-entry
 * outcome on the login server. Covers success and every failure mode (wrong
 * password, banned, IP-restricted, etc.). Emitted independently of whether the
 * account proceeds to character selection / world entry.
 *
 * <p>Partition key on the wire is {@code accountName.toLowerCase(Locale.ROOT)}
 * so per-account attempt history lands in one partition in occurrence order.
 * The platform consumes this stream into account-watchlist alerts.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7 String. Idempotency key for
 *   at-least-once delivery; platform extracts {@code occurredAt} from the
 *   time-ordered prefix.</li>
 *   <li>{@link #getServerId() serverId} — Login-server UUID String. Identifies
 *   the originating LS instance.</li>
 *   <li>{@link #getAccountName() accountName} — REQUIRED. Lowercased and
 *   trimmed by the producer.</li>
 *   <li>{@link #getClientIp() clientIp} — REQUIRED. Client IP captured at the
 *   moment credentials were submitted.</li>
 *   <li>{@link #getHwid() hwid} — optional hardware-id (build-specific
 *   format); always {@code null} on bohpts current protocol — reserved for
 *   cores that carry HWID on the login flow.</li>
 *   <li>{@link #getOutcome() outcome} — REQUIRED. See javadoc on the
 *   accessor.</li>
 *   <li>{@link #getAttemptedAt() attemptedAt} — REQUIRED. UTC wall-clock at
 *   the moment the attempt was evaluated.</li>
 *   <li>{@link #getFailureDetail() failureDetail} — optional free-form
 *   diagnostic string. Never carries secrets (no password, no hash).</li>
 * </ul>
 */
public final class AccountAuthAttemptEvent {

    private final String eventId;
    private final String serverId;
    private final String accountName;
    private final String clientIp;
    private final @Nullable String hwid;
    private final String outcome;
    private final Instant attemptedAt;
    private final @Nullable String failureDetail;

    public AccountAuthAttemptEvent(String eventId,
                                   String serverId,
                                   String accountName,
                                   String clientIp,
                                   @Nullable String hwid,
                                   String outcome,
                                   Instant attemptedAt,
                                   @Nullable String failureDetail) {
        this.eventId = Objects.requireNonNull(eventId, "AccountAuthAttemptEvent.eventId is required");
        this.serverId = Objects.requireNonNull(serverId, "AccountAuthAttemptEvent.serverId is required");
        this.accountName = Objects.requireNonNull(accountName, "AccountAuthAttemptEvent.accountName is required");
        this.clientIp = Objects.requireNonNull(clientIp, "AccountAuthAttemptEvent.clientIp is required");
        this.hwid = hwid;
        this.outcome = Objects.requireNonNull(outcome, "AccountAuthAttemptEvent.outcome is required");
        this.attemptedAt = Objects.requireNonNull(attemptedAt, "AccountAuthAttemptEvent.attemptedAt is required");
        this.failureDetail = failureDetail;
    }

    public String getEventId() {
        return eventId;
    }

    public String getServerId() {
        return serverId;
    }

    public String getAccountName() {
        return accountName;
    }

    public String getClientIp() {
        return clientIp;
    }

    public @Nullable String getHwid() {
        return hwid;
    }

    /**
     * Free-form String; see {@link AuthOutcomes} for known values. Consumers
     * MUST handle unknown values gracefully — unknown outcomes don't match
     * any rule and are not an error.
     */
    public String getOutcome() {
        return outcome;
    }

    public Instant getAttemptedAt() {
        return attemptedAt;
    }

    public @Nullable String getFailureDetail() {
        return failureDetail;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .serverId(serverId)
                .accountName(accountName)
                .clientIp(clientIp)
                .hwid(hwid)
                .outcome(outcome)
                .attemptedAt(attemptedAt)
                .failureDetail(failureDetail);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AccountAuthAttemptEvent)) return false;
        AccountAuthAttemptEvent that = (AccountAuthAttemptEvent) o;
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(clientIp, that.clientIp)
                && Objects.equals(hwid, that.hwid)
                && Objects.equals(outcome, that.outcome)
                && Objects.equals(attemptedAt, that.attemptedAt)
                && Objects.equals(failureDetail, that.failureDetail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, serverId, accountName, clientIp, hwid, outcome,
                attemptedAt, failureDetail);
    }

    @Override
    public String toString() {
        return "AccountAuthAttemptEvent[eventId=" + eventId
                + ", serverId=" + serverId
                + ", accountName=" + accountName
                + ", clientIp=" + clientIp
                + ", hwid=" + hwid
                + ", outcome=" + outcome
                + ", attemptedAt=" + attemptedAt
                + ", failureDetail=" + failureDetail + "]";
    }

    public static final class Builder {
        private @Nullable String eventId;
        private @Nullable String serverId;
        private @Nullable String accountName;
        private @Nullable String clientIp;
        private @Nullable String hwid;
        private @Nullable String outcome;
        private @Nullable Instant attemptedAt;
        private @Nullable String failureDetail;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder serverId(String serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder clientIp(String clientIp) {
            this.clientIp = clientIp;
            return this;
        }

        public Builder hwid(@Nullable String hwid) {
            this.hwid = hwid;
            return this;
        }

        public Builder outcome(String outcome) {
            this.outcome = outcome;
            return this;
        }

        public Builder attemptedAt(Instant attemptedAt) {
            this.attemptedAt = attemptedAt;
            return this;
        }

        public Builder failureDetail(@Nullable String failureDetail) {
            this.failureDetail = failureDetail;
            return this;
        }

        public AccountAuthAttemptEvent build() {
            return new AccountAuthAttemptEvent(eventId, serverId, accountName, clientIp,
                    hwid, outcome, attemptedAt, failureDetail);
        }
    }
}
