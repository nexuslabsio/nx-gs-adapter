package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import java.nio.charset.StandardCharsets;

/**
 * Reply type is pre-encoded once: command simple name minus {@code Command} suffix plus {@code Result}
 * (e.g. {@code TransferItemToCharacterCommand} to {@code TransferItemToCharacterResult}).
 */
final class CommandTypeBinding {

    private final Class<? extends NxCommand<?>> commandClass;
    private final byte[] replyMessageTypeBytes;

    @SuppressWarnings("rawtypes")
    private final CommandHandler handler;

    @SuppressWarnings("rawtypes")
    CommandTypeBinding(Class<? extends NxCommand<?>> commandClass, CommandHandler handler) {
        this.commandClass = commandClass;
        this.replyMessageTypeBytes =
                deriveReplyTypeName(commandClass.getSimpleName()).getBytes(StandardCharsets.UTF_8);
        this.handler = handler;
    }

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
