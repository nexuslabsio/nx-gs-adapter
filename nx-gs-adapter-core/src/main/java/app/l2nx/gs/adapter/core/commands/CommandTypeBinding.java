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
 * the consumer's hot path does not re-build the wire reply type on every
 * reply. Naming convention: strip the {@code "Command"} suffix from the
 * command's simple name and append {@code "Result"} — e.g.
 * {@code TransferItemToCharacterCommand} →
 * {@code TransferItemToCharacterResult}, matching the sibling result
 * payload class.</p>
 */
final class CommandTypeBinding {

    private final Class<? extends NxCommand<?>> commandClass;
    private final byte[] replyMessageTypeBytes;
    @SuppressWarnings("rawtypes")
    private final CommandHandler handler;

    @SuppressWarnings("rawtypes")
    CommandTypeBinding(Class<? extends NxCommand<?>> commandClass,
                       CommandHandler handler) {
        this.commandClass = commandClass;
        this.replyMessageTypeBytes = deriveReplyTypeName(commandClass.getSimpleName())
                .getBytes(StandardCharsets.UTF_8);
        this.handler = handler;
    }

    /**
     * Strip the {@code "Command"} suffix (if present) and append
     * {@code "Result"}. Package-visible so {@link CommandsConsumer} can
     * use the same derivation when replying with no resolved binding.
     */
    static String deriveReplyTypeName(String commandSimpleName) {
        String stripped = commandSimpleName.endsWith("Command")
                ? commandSimpleName.substring(0, commandSimpleName.length() - "Command".length())
                : commandSimpleName;
        return stripped + "Result";
    }

    Class<? extends NxCommand<?>> commandClass() {
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
