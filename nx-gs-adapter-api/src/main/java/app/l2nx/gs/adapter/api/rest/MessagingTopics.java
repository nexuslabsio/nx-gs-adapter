package app.l2nx.gs.adapter.api.rest;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Top-level Kafka topic addressing for the bidirectional messaging surface
 * between the game-server adapter and the platform's web side. Returned in
 * {@link ConnectResponse}, parallel to (and independent of) {@link SyncTopics}
 * which handles the DB / runtime / datapack sync streams.
 *
 * <p>Two coexisting maps:</p>
 * <ul>
 *     <li>{@link #getEvents()} — outbound discrete-fact streams from core to
 *     platform. Per-family fully-qualified Kafka topic
 *     ({@code <tenant>.gs.events.<family>}). Phase-1 family: {@code premium}.
 *     Phase-2 reserved keys: {@code character}, {@code clan}, {@code server}.</li>
 *     <li>{@link #getCommands()} — inbound per-business-domain command streams
 *     from platform to core
 *     ({@code <tenant>.gs.commands.<domain>}). Phase-2 reserved keys: {@code char},
 *     {@code clan}, {@code mail}, {@code account}. Phase-1 ships an empty map —
 *     the wire slot is reserved so Phase-2 doesn't require a {@code ConnectResponse}
 *     schema bump.</li>
 * </ul>
 *
 * <p>Each map is defensively copied on construction and exposed unmodifiable.
 * {@code null} on a getter is normalized to an empty map at the read site so
 * Gson deserialization (which bypasses the constructor when the field is
 * absent on the wire) does not break the contract.</p>
 */
public final class MessagingTopics {

    private final Map<String, String> events;
    private final Map<String, String> commands;

    public MessagingTopics(@Nullable Map<String, String> events,
                           @Nullable Map<String, String> commands) {
        this.events = freeze(events);
        this.commands = freeze(commands);
    }

    /**
     * Outbound events: family → fully-qualified Kafka topic. Always non-null —
     * {@code freeze()} normalizes a {@code null} constructor argument to an
     * empty map. Empty means no event families are configured (every
     * {@code NxEvents.publishX(...)} becomes a no-op + DEBUG log).
     */
    public Map<String, String> getEvents() {
        return events;
    }

    /**
     * Inbound commands: domain → fully-qualified Kafka topic. Always non-null —
     * see {@link #getEvents()} for the null-normalization contract. Empty
     * means commands inbound is disabled (the dominant Phase-1 configuration).
     */
    public Map<String, String> getCommands() {
        return commands;
    }

    public Builder toBuilder() {
        return new Builder().events(events).commands(commands);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, String> freeze(@Nullable Map<String, String> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessagingTopics)) return false;
        MessagingTopics that = (MessagingTopics) o;
        return Objects.equals(events, that.events) && Objects.equals(commands, that.commands);
    }

    @Override
    public int hashCode() {
        return Objects.hash(events, commands);
    }

    @Override
    public String toString() {
        return "MessagingTopics[events=" + events + ", commands=" + commands + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, String> events;
        private @Nullable Map<String, String> commands;

        public Builder events(@Nullable Map<String, String> events) {
            this.events = events;
            return this;
        }

        public Builder commands(@Nullable Map<String, String> commands) {
            this.commands = commands;
            return this;
        }

        public MessagingTopics build() {
            return new MessagingTopics(events, commands);
        }
    }
}
