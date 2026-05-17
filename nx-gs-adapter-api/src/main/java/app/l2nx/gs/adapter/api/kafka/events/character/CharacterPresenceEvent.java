package app.l2nx.gs.adapter.api.kafka.events.character;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

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
 *   <li>{@link #getAccountName() accountName} — optional; login account
 *   owning this character at the moment of the event.</li>
 *   <li>{@link #getIp() ip} — optional client IP captured at the event.</li>
 *   <li>{@link #getHwid() hwid} — optional hardware-id (build-specific
 *   format); only carries on cores with HWID tracking.</li>
 * </ul>
 */
public final class CharacterPresenceEvent {

    private final UUID eventId;
    private final long charId;
    private final boolean online;
    private final @Nullable String accountName;
    private final @Nullable String ip;
    private final @Nullable String hwid;

    public CharacterPresenceEvent(UUID eventId,
                                  long charId,
                                  boolean online,
                                  @Nullable String accountName,
                                  @Nullable String ip,
                                  @Nullable String hwid) {
        this.eventId = Objects.requireNonNull(eventId, "CharacterPresenceEvent.eventId is required");
        this.charId = charId;
        this.online = online;
        this.accountName = accountName;
        this.ip = ip;
        this.hwid = hwid;
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

    public @Nullable String getAccountName() {
        return accountName;
    }

    public @Nullable String getIp() {
        return ip;
    }

    public @Nullable String getHwid() {
        return hwid;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .charId(charId)
                .online(online)
                .accountName(accountName)
                .ip(ip)
                .hwid(hwid);
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
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(ip, that.ip)
                && Objects.equals(hwid, that.hwid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, online, accountName, ip, hwid);
    }

    @Override
    public String toString() {
        return "CharacterPresenceEvent[eventId=" + eventId
                + ", charId=" + charId
                + ", online=" + online
                + ", accountName=" + accountName
                + ", ip=" + ip
                + ", hwid=" + hwid + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long charId;
        private boolean online;
        private @Nullable String accountName;
        private @Nullable String ip;
        private @Nullable String hwid;

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

        public CharacterPresenceEvent build() {
            return new CharacterPresenceEvent(eventId, charId, online, accountName, ip, hwid);
        }
    }
}
