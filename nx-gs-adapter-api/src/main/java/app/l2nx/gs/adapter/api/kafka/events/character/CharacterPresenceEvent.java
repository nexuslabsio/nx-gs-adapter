package app.l2nx.gs.adapter.api.kafka.events.character;

import app.l2nx.gs.adapter.api.kafka.events.character.model.WellKnownPresenceMetadata;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * One event per login ({@code online=true}) or logout, emitted from the standard packet path. Cheat/custom clients
 * bypassing it are covered by the CDC and runtime channels, the other two sources reconciled into
 * {@code gs_characters.online} with timestamp-based last-writer-wins; {@code eventId} is a UUIDv7 so {@code occurredAt}
 * is extracted from its prefix. {@code charId} is also the partition key.
 * {@code sessionId}: a login and its matching logout carry the SAME id, fresh per login-session; {@code null} on builds
 * that do not emit it, in which case the platform leaves the session's logout time unset.
 * {@code hwid} only on cores with HWID tracking. {@code metadata}: open map, {@code null} when absent; canonical keys in
 * {@link WellKnownPresenceMetadata} (today {@code logout_reason=disconnect} for involuntary connection loss).
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
