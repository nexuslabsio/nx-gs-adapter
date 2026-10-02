package app.l2nx.gs.adapter.api.kafka.events.account;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One event per credential-entry outcome on the login server, success or any failure, independent of whether the
 * account proceeds to world entry. Partition key is {@code accountName.toLowerCase(Locale.ROOT)} (per-account order).
 * {@code eventId} is a UUIDv7 idempotency key (at-least-once). {@code accountName} is lowercased and trimmed by the
 * producer. {@code hwid} is always {@code null} for hosts whose protocol lacks it; {@code failureDetail} never carries secrets.
 * {@code metadata} is an open map; hosts MAY add keys without an API release, consumers ignore unknown ones.
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
    private final @Nullable Map<String, String> metadata;

    public AccountAuthAttemptEvent(
            String eventId,
            String serverId,
            String accountName,
            String clientIp,
            @Nullable String hwid,
            String outcome,
            Instant attemptedAt,
            @Nullable String failureDetail,
            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "AccountAuthAttemptEvent.eventId is required");
        this.serverId = Objects.requireNonNull(serverId, "AccountAuthAttemptEvent.serverId is required");
        this.accountName = Objects.requireNonNull(accountName, "AccountAuthAttemptEvent.accountName is required");
        this.clientIp = Objects.requireNonNull(clientIp, "AccountAuthAttemptEvent.clientIp is required");
        this.hwid = hwid;
        this.outcome = Objects.requireNonNull(outcome, "AccountAuthAttemptEvent.outcome is required");
        this.attemptedAt = Objects.requireNonNull(attemptedAt, "AccountAuthAttemptEvent.attemptedAt is required");
        this.failureDetail = failureDetail;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
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

    /** Free-form; see {@link AuthOutcomes}. Consumers MUST tolerate unknown values (they match no rule, not an error). */
    public String getOutcome() {
        return outcome;
    }

    public Instant getAttemptedAt() {
        return attemptedAt;
    }

    public @Nullable String getFailureDetail() {
        return failureDetail;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
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
                .failureDetail(failureDetail)
                .metadata(metadata);
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
                && Objects.equals(failureDetail, that.failureDetail)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eventId, serverId, accountName, clientIp, hwid, outcome, attemptedAt, failureDetail, metadata);
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
                + ", failureDetail=" + failureDetail
                + ", metadata=" + metadata + "]";
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
        private @Nullable Map<String, String> metadata;

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

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public AccountAuthAttemptEvent build() {
            return new AccountAuthAttemptEvent(
                    eventId, serverId, accountName, clientIp, hwid, outcome, attemptedAt, failureDetail, metadata);
        }
    }
}
