package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jspecify.annotations.Nullable;

/**
 * Routing matches on {@code Class.getSimpleName()}; two classes with the same simple name collide and the
 * last registration wins. Registration may race with lookup on the consumer thread.
 */
final class CommandTypeRegistry {

    private final ConcurrentMap<String, CommandTypeBinding> bindingsByMessageType =
            new ConcurrentHashMap<String, CommandTypeBinding>();

    /** @return {@code true} if a previous binding with the same simple name was overwritten */
    <R, C extends NxCommand<R>> boolean register(Class<C> type, CommandHandler<C, R> handler) {
        CommandTypeBinding binding = new CommandTypeBinding(type, handler);
        CommandTypeBinding previous = bindingsByMessageType.put(type.getSimpleName(), binding);
        return previous != null;
    }

    @Nullable
    CommandTypeBinding lookup(String messageType) {
        if (messageType == null) {
            return null;
        }
        return bindingsByMessageType.get(messageType);
    }

    /** Sorted for stable heartbeat output. */
    List<String> snapshotRegisteredTypes() {
        if (bindingsByMessageType.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<String>(bindingsByMessageType.keySet());
        Collections.sort(names);
        return Collections.unmodifiableList(names);
    }
}
