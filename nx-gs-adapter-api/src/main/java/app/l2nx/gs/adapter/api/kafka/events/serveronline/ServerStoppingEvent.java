package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import app.l2nx.gs.adapter.api.kafka.events.serveronline.model.WellKnownServerStartMetadata;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Emitted once on graceful shutdown, before the server stops accepting logins; shares the
 * {@code serveronline} topic with {@link ServerOnlineSnapshotEvent} and {@link ServerStartedEvent}.
 *
 * <p>Graceful only: a crash emits nothing and is caught by the platform's heartbeat timeout. The host
 * suppresses it during scheduled restarts. {@code eventId} is a UUIDv7 idempotency key. {@code metadata}
 * carries {@link WellKnownServerStartMetadata#GM_ONLY}; the host always reports it and the platform decides
 * whether to suppress the notification.</p>
 *
 * <p>Partition key is {@code null} (round-robin).</p>
 */
public final class ServerStoppingEvent {

    private final UUID eventId;
    private final @Nullable Map<String, String> metadata;

    public ServerStoppingEvent(UUID eventId, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "ServerStoppingEvent.eventId is required");
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServerStoppingEvent)) return false;
        ServerStoppingEvent that = (ServerStoppingEvent) o;
        return eventId.equals(that.eventId) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, metadata);
    }

    @Override
    public String toString() {
        return "ServerStoppingEvent[eventId=" + eventId + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public ServerStoppingEvent build() {
            return new ServerStoppingEvent(eventId, metadata);
        }
    }
}
