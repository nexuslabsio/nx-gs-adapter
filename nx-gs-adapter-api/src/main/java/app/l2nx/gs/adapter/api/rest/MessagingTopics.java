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
 * <p>Three coexisting addressing slots:</p>
 * <ul>
 *     <li>{@link #getEvents()} — outbound discrete-fact streams from core to
 *     platform. Per-family fully-qualified Kafka topic
 *     ({@code <tenant>.gs.events.<family>}). Phase-1 family: {@code premium}.
 *     Phase-2 reserved keys: {@code character}, {@code clan}, {@code server}.</li>
 *     <li>{@link #getCommandsTopic()} — single inbound topic for all command
 *     types (e.g. {@code <tenant>.gs.commands}). Phase-2 contract: cross-domain
 *     ordering per character is preserved by partitioning records on
 *     {@code charId}; web side resolves human identifiers to {@code charId}
 *     before sending. Routing inside the topic is by {@code Nx-Message-Type}
 *     Kafka header.</li>
 *     <li>{@link #getCommandsRepliesTopic()} — single outbound topic for
 *     command replies (e.g. {@code <tenant>.gs.commands.replies}). Each reply
 *     carries the inbound {@code Nx-Correlation-Id} header so the platform
 *     side can route it back to the originating web request.</li>
 * </ul>
 *
 * <p>The {@code events} map is defensively copied on construction and exposed
 * unmodifiable. {@code null} on a getter is normalized to an empty map at the
 * read site so Gson deserialization (which bypasses the constructor when the
 * field is absent on the wire) does not break the contract.</p>
 *
 * <p><b>Wire-shape evolution.</b> Phase-1 shipped a {@code commands: Map<String,String>}
 * placeholder for per-domain topics; Phase-4 replaces it with the single
 * {@code commandsTopic} + {@code commandsRepliesTopic} pair after design dialog
 * concluded that cross-character cross-domain ordering on a single character
 * is the more valuable invariant than per-domain isolation. A 0.13.x platform
 * still shipping {@code "commands": {}} on the wire is harmless under Gson's
 * default ignore-unknown-fields behaviour.</p>
 */
public final class MessagingTopics {

    private final Map<String, String> events;
    private final @Nullable String commandsTopic;
    private final @Nullable String commandsRepliesTopic;

    public MessagingTopics(@Nullable Map<String, String> events,
                           @Nullable String commandsTopic,
                           @Nullable String commandsRepliesTopic) {
        this.events = freeze(events);
        this.commandsTopic = isPresent(commandsTopic) ? commandsTopic : null;
        this.commandsRepliesTopic = isPresent(commandsRepliesTopic) ? commandsRepliesTopic : null;
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
     * Single inbound commands topic. {@code null} (or blank on the wire) means
     * commands inbound is disabled — adapter-core does not start the consumer
     * thread. Host registrations via {@code ctx.commands().on(...)} are still
     * accepted but never invoked.
     */
    public @Nullable String getCommandsTopic() {
        return commandsTopic;
    }

    /**
     * Single outbound topic for {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}
     * replies. {@code null} (or blank on the wire) means replies are disabled —
     * handlers run, but reply records cannot be published. Useful for
     * fire-and-forget admin commands; in normal operation web side correlates
     * by {@code Nx-Correlation-Id} on this topic.
     */
    public @Nullable String getCommandsRepliesTopic() {
        return commandsRepliesTopic;
    }

    public Builder toBuilder() {
        return new Builder()
                .events(events)
                .commandsTopic(commandsTopic)
                .commandsRepliesTopic(commandsRepliesTopic);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, String> freeze(@Nullable Map<String, String> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, String>(src));
    }

    private static boolean isPresent(@Nullable String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessagingTopics)) return false;
        MessagingTopics that = (MessagingTopics) o;
        return Objects.equals(events, that.events)
                && Objects.equals(commandsTopic, that.commandsTopic)
                && Objects.equals(commandsRepliesTopic, that.commandsRepliesTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(events, commandsTopic, commandsRepliesTopic);
    }

    @Override
    public String toString() {
        return "MessagingTopics[events=" + events
                + ", commandsTopic=" + commandsTopic
                + ", commandsRepliesTopic=" + commandsRepliesTopic + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, String> events;
        private @Nullable String commandsTopic;
        private @Nullable String commandsRepliesTopic;

        public Builder events(@Nullable Map<String, String> events) {
            this.events = events;
            return this;
        }

        public Builder commandsTopic(@Nullable String commandsTopic) {
            this.commandsTopic = commandsTopic;
            return this;
        }

        public Builder commandsRepliesTopic(@Nullable String commandsRepliesTopic) {
            this.commandsRepliesTopic = commandsRepliesTopic;
            return this;
        }

        public MessagingTopics build() {
            return new MessagingTopics(events, commandsTopic, commandsRepliesTopic);
        }
    }
}
