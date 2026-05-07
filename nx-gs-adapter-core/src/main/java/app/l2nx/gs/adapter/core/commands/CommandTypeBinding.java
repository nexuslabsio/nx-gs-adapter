package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandHandler;

import java.nio.charset.StandardCharsets;

/**
 * Internal binding from a registered {@link NxCommand} class to its handler
 * and pre-cached wire metadata. Created at registration time by
 * {@link CommandTypeRegistry} and looked up at dispatch time by the consumer
 * thread.
 *
 * <p>Holds the raw class for Gson deserialization and a wildcard handler
 * reference; the dispatch site casts back at the call boundary. The cast is
 * safe because the class is what populated the binding in the first place.</p>
 *
 * <p>{@link #replyMessageTypeBytes()} is pre-encoded once at registration so
 * the consumer's hot path does not re-build {@code (simpleName + "Result")}
 * UTF-8 bytes on every reply — mirrors {@code EventTypeBinding.messageTypeBytes}.</p>
 */
final class CommandTypeBinding {

    private final Class<? extends NxCommand> commandClass;
    private final byte[] replyMessageTypeBytes;
    @SuppressWarnings("rawtypes")
    private final CommandHandler handler;

    @SuppressWarnings("rawtypes")
    CommandTypeBinding(Class<? extends NxCommand> commandClass,
                       CommandHandler handler) {
        this.commandClass = commandClass;
        this.replyMessageTypeBytes = (commandClass.getSimpleName() + "Result")
                .getBytes(StandardCharsets.UTF_8);
        this.handler = handler;
    }

    Class<? extends NxCommand> commandClass() {
        return commandClass;
    }

    byte[] replyMessageTypeBytes() {
        return replyMessageTypeBytes;
    }

    @SuppressWarnings("rawtypes")
    CommandHandler handler() {
        return handler;
    }
}
