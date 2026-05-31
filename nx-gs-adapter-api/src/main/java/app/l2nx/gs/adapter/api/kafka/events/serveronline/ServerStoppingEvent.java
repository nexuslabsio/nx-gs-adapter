package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Discrete server-lifecycle fact — emitted once on the graceful-shutdown path,
 * before the server stops accepting logins. Joins {@link ServerOnlineSnapshotEvent}
 * and {@link ServerStartedEvent} on the {@code serveronline} family
 * ({@code <tenant>.gs.events.serveronline}); dispatched by the
 * {@code Nx-Message-Type} header.
 *
 * <p>Covers <b>graceful</b> shutdown only — a hard crash leaves no JVM to emit
 * anything, so server-down detection for crashes falls to the platform's
 * heartbeat-timeout mechanism (separate, existing). The host suppresses this
 * event during its scheduled maintenance restart window (see
 * {@link ServerStartedEvent}).</p>
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency key; the
 *   platform extracts {@code occurredAt} from the time-ordered prefix.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map; same
 *   canonical key as {@link ServerStartedEvent}: {@code gm_only} ("true"/"false")
 *   via {@link WellKnownServerStartMetadata#GM_ONLY}. The host always reports the
 *   server's GM-only state; the <b>platform</b> decides whether to suppress the
 *   "server is stopping" notification when {@code gm_only=true} (GM-only runs are
 *   operator tests, often several restarts in a row).</li>
 * </ul>
 *
 * <p>Partition key: {@code null} (round-robin).</p>
 */
public final class ServerStoppingEvent {

    private final UUID eventId;
    private final @Nullable Map<String, String> metadata;

    public ServerStoppingEvent(UUID eventId,
                               @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "ServerStoppingEvent.eventId is required");
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServerStoppingEvent)) return false;
        ServerStoppingEvent that = (ServerStoppingEvent) o;
        return eventId.equals(that.eventId)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, metadata);
    }

    @Override
    public String toString() {
        return "ServerStoppingEvent[eventId=" + eventId
                + ", metadata=" + metadata + "]";
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
