package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import app.l2nx.gs.adapter.api.kafka.events.serveronline.model.WellKnownServerStartMetadata;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Emitted once when the server has finished loading the world and is accepting logins; shares the
 * {@code serveronline} topic with {@link ServerOnlineSnapshotEvent} (dispatched by {@code Nx-Message-Type}).
 *
 * <p>The host owns suppression during scheduled maintenance restarts; the platform applies no restart
 * logic. {@code eventId} is a UUIDv7 idempotency key. {@code metadata} carries {@link WellKnownServerStartMetadata}
 * keys; a consumer SHOULD mute its "server is up" notification when {@code gm_only=true}.</p>
 *
 * <p>Partition key is {@code null} (round-robin); consumers group by the {@code Nx-Server-Id} header.</p>
 */
public final class ServerStartedEvent {

    private final UUID eventId;
    private final @Nullable Map<String, String> metadata;

    public ServerStartedEvent(UUID eventId, @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "ServerStartedEvent.eventId is required");
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
        if (!(o instanceof ServerStartedEvent)) return false;
        ServerStartedEvent that = (ServerStartedEvent) o;
        return eventId.equals(that.eventId) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, metadata);
    }

    @Override
    public String toString() {
        return "ServerStartedEvent[eventId=" + eventId + ", metadata=" + metadata + "]";
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

        public ServerStartedEvent build() {
            return new ServerStartedEvent(eventId, metadata);
        }
    }
}
