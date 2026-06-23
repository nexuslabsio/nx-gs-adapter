package app.l2nx.gs.adapter.api.kafka.events.character;

import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Discrete presence-change fact — one event per login or logout, emitted
 * from the standard packet path on the host. {@code online=true} encodes
 * login, {@code online=false} encodes logout. Single type because login
 * and logout share emission point, payload shape, partition key,
 * reconciliation rules, and topic — the differentiator is one bit.
 *
 * <p>One of three sources reconciled into {@code gs_characters.online} on
 * the platform (others: CDC {@code CharacterDbDto.online} via 60s poll
 * cycle, and runtime {@code CharacterRuntimeDto.online} via in-memory
 * tombstones). Timestamp-based last-writer-wins resolves cross-source races;
 * {@link #getEventId() eventId} is UUIDv7 so {@code occurredAt} extracts
 * cleanly from its time-ordered prefix.</p>
 *
 * <p>Cheat / custom clients bypassing the standard packet flow won't trigger
 * this event — CDC and runtime channels act as the always-present fallback.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency key
 *   for at-least-once delivery; platform extracts {@code occurredAt} from
 *   the time-ordered prefix for last-writer-wins ordering.</li>
 *   <li>{@link #getCharId() charId} — REQUIRED. Also serves as the Kafka
 *   partition key so per-character history lands on one partition in
 *   occurrence order.</li>
 *   <li>{@link #isOnline() online} — REQUIRED. {@code true} = login,
 *   {@code false} = logout.</li>
 *   <li>{@link #getSessionId() sessionId} — optional wire correlation key for
 *   a login/logout pair. A login fact and its matching logout fact carry the
 *   SAME {@code sessionId}; the host mints a FRESH unique {@code sessionId} per
 *   login-session. {@code null} on builds that do not emit it — the platform
 *   then cannot correlate the logout and leaves the session's logout time
 *   unset.</li>
 *   <li>{@link #getAccountName() accountName} — optional; login account
 *   owning this character at the moment of the event.</li>
 *   <li>{@link #getIp() ip} — optional client IP captured at the event.</li>
 *   <li>{@link #getHwid() hwid} — optional hardware-id (build-specific
 *   format); only carries on cores with HWID tracking.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic attributes about this presence change. {@code null} when
 *   absent (the common path). Canonical keys/values are documented in
 *   {@link WellKnownPresenceMetadata}; the one defined today is
 *   {@code logout_reason=disconnect}, set on logout events that were caused
 *   by an involuntary connection loss. Hosts MAY publish arbitrary
 *   non-canonical keys without an API release; consumers ignore keys they do
 *   not understand.</li>
 * </ul>
 */
public final class CharacterPresenceEvent {

    private final UUID eventId;
    private final long charId;
    private final boolean online;
    private final @Nullable UUID sessionId;
    private final @Nullable String accountName;
    private final @Nullable String ip;
    private final @Nullable String hwid;
    private final @Nullable Map<String, String> metadata;

    public CharacterPresenceEvent(
            UUID eventId,
            long charId,
            boolean online,
            @Nullable UUID sessionId,
            @Nullable String accountName,
            @Nullable String ip,
            @Nullable String hwid,
            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "CharacterPresenceEvent.eventId is required");
        this.charId = charId;
        this.online = online;
        this.sessionId = sessionId;
        this.accountName = accountName;
        this.ip = ip;
        this.hwid = hwid;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharId() {
        return charId;
    }

    public boolean isOnline() {
        return online;
    }

    public @Nullable UUID getSessionId() {
        return sessionId;
    }

    public @Nullable String getAccountName() {
        return accountName;
    }

    public @Nullable String getIp() {
        return ip;
    }

    public @Nullable String getHwid() {
        return hwid;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .charId(charId)
                .online(online)
                .sessionId(sessionId)
                .accountName(accountName)
                .ip(ip)
                .hwid(hwid)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterPresenceEvent)) return false;
        CharacterPresenceEvent that = (CharacterPresenceEvent) o;
        return charId == that.charId
                && online == that.online
                && eventId.equals(that.eventId)
                && Objects.equals(sessionId, that.sessionId)
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(ip, that.ip)
                && Objects.equals(hwid, that.hwid)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, online, sessionId, accountName, ip, hwid, metadata);
    }

    @Override
    public String toString() {
        return "CharacterPresenceEvent[eventId=" + eventId
                + ", charId=" + charId
                + ", online=" + online
                + ", sessionId=" + sessionId
                + ", accountName=" + accountName
                + ", ip=" + ip
                + ", hwid=" + hwid
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long charId;
        private boolean online;
        private @Nullable UUID sessionId;
        private @Nullable String accountName;
        private @Nullable String ip;
        private @Nullable String hwid;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder online(boolean online) {
            this.online = online;
            return this;
        }

        public Builder sessionId(@Nullable UUID sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder accountName(@Nullable String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder ip(@Nullable String ip) {
            this.ip = ip;
            return this;
        }

        public Builder hwid(@Nullable String hwid) {
            this.hwid = hwid;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CharacterPresenceEvent build() {
            return new CharacterPresenceEvent(eventId, charId, online, sessionId, accountName, ip, hwid, metadata);
        }
    }
}
