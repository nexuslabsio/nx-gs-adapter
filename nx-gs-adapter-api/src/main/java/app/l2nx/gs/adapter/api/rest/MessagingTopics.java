package app.l2nx.gs.adapter.api.rest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Kafka topic addressing for events and commands, returned in {@link ConnectResponse}.
 * <ul>
 *     <li>{@link #getEvents()} - family to fully-qualified topic ({@code <tenant>.gs.events.<family>}).</li>
 *     <li>{@link #getCommandsTopic()} - single inbound topic ({@code <tenant>.gs.commands}). The record key is a nullable {@code Long} (charId for character-scoped commands, preserving per-character ordering; unkeyed otherwise) and is never read by the adapter. Routing uses {@code Nx-Message-Type} and {@code Nx-Target-Server-Id}.</li>
 *     <li>{@link #getCommandsRepliesTopic()} - single reply topic ({@code <tenant>.gs.commands.replies}); replies carry the inbound {@code Nx-Correlation-Id}.</li>
 * </ul>
 * The {@code events} map is copied defensively and exposed unmodifiable; getters normalize {@code null} (Gson bypasses the constructor) to empty. A legacy {@code "commands": {}} on the wire is ignored.
 */
public final class MessagingTopics {

    private final Map<String, String> events;
    private final @Nullable String commandsTopic;
    private final @Nullable String commandsRepliesTopic;

    public MessagingTopics(
            @Nullable Map<String, String> events,
            @Nullable String commandsTopic,
            @Nullable String commandsRepliesTopic) {
        this.events = freeze(events);
        this.commandsTopic = isPresent(commandsTopic) ? commandsTopic : null;
        this.commandsRepliesTopic = isPresent(commandsRepliesTopic) ? commandsRepliesTopic : null;
    }

    /** Never null; empty means no event families are configured (publishing is a no-op). */
    public Map<String, String> getEvents() {
        return events;
    }

    /** {@code null} or blank disables the command consumer; registered handlers are never invoked. */
    public @Nullable String getCommandsTopic() {
        return commandsTopic;
    }

    /** {@code null} or blank disables replies: handlers still run but nothing is published. */
    public @Nullable String getCommandsRepliesTopic() {
        return commandsRepliesTopic;
    }

    public Builder toBuilder() {
        return new Builder().events(events).commandsTopic(commandsTopic).commandsRepliesTopic(commandsRepliesTopic);
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
