package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Discrete server-lifecycle fact — emitted once when the game server has
 * finished loading the world and is accepting (or about to accept) logins.
 * Joins {@link ServerOnlineSnapshotEvent} on the {@code serveronline} family
 * ({@code <tenant>.gs.events.serveronline}); the two share a topic and are
 * dispatched by the {@code Nx-Message-Type} header.
 *
 * <p>The host owns suppression of this event during scheduled maintenance
 * restarts — it simply does not emit a started / stopping event inside its
 * restart window. The platform applies no restart-time logic.</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency key;
 *   platform extracts {@code occurredAt} from the time-ordered prefix.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic startup attributes; {@code null} when absent. Canonical key
 *   in {@link WellKnownServerStartMetadata}: {@code gm_only} ({@code "true"} /
 *   {@code "false"}) — whether the server started in GM-only mode. A consumer
 *   SHOULD mute its "server is up" notification when {@code gm_only=true}
 *   (a GM-only startup is a maintenance state, not an "open for players"
 *   announcement). Hosts MAY publish arbitrary non-canonical keys.</li>
 * </ul>
 *
 * <p>Partition key: {@code null} (round-robin); ordering per server is preserved
 * via the UUIDv7 {@code eventId} timestamp, consumers group by the
 * {@code Nx-Server-Id} header.</p>
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
