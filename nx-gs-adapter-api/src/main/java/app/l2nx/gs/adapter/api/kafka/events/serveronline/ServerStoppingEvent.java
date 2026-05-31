package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

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
 * <p>Carries only {@link #getEventId() eventId} — UUIDv7, REQUIRED. Idempotency
 * key; the platform extracts {@code occurredAt} from the time-ordered prefix.
 * No stop-reason classification on the wire — the fact "the server is going
 * down" is the whole signal.</p>
 *
 * <p>Partition key: {@code null} (round-robin).</p>
 */
public final class ServerStoppingEvent {

    private final UUID eventId;

    public ServerStoppingEvent(UUID eventId) {
        this.eventId = Objects.requireNonNull(eventId, "ServerStoppingEvent.eventId is required");
    }

    public UUID getEventId() {
        return eventId;
    }

    public Builder toBuilder() {
        return new Builder().eventId(eventId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServerStoppingEvent)) return false;
        ServerStoppingEvent that = (ServerStoppingEvent) o;
        return eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }

    @Override
    public String toString() {
        return "ServerStoppingEvent[eventId=" + eventId + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public ServerStoppingEvent build() {
            return new ServerStoppingEvent(eventId);
        }
    }
}
