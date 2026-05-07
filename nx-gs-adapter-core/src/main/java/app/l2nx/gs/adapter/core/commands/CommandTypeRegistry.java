package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandHandler;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Host-populated {@code Nx-Message-Type} → handler binding table. Populated
 * via {@link app.l2nx.gs.adapter.api.spi.NxCommands#on(Class, CommandHandler)}
 * calls from host {@code onConnect} callbacks (and any time afterwards — late
 * registration is supported).
 *
 * <p>Routing matches by {@code Class.getSimpleName()} (UTF-8 string). Two
 * distinct classes with the same simple name in the catalog would collide
 * — last registration wins (logged as WARN, the prior binding is replaced).</p>
 *
 * <p>Concurrent: registrations may arrive while the consumer thread is
 * looking up by header. Backed by {@link ConcurrentHashMap} so reads and
 * writes do not stall each other.</p>
 *
 * <p>Package-private. External callers go through
 * {@link app.l2nx.gs.adapter.api.spi.NxCommands} (registration) or do not
 * see this class at all (dispatch is internal).</p>
 */
final class CommandTypeRegistry {

    private final ConcurrentMap<String, CommandTypeBinding> bindingsByMessageType =
            new ConcurrentHashMap<String, CommandTypeBinding>();

    /**
     * Register a handler for the given concrete command class. Overwrites
     * any previous registration for the same class (last write wins).
     *
     * @return {@code true} if a previous binding for the same class simple
     * name was overwritten; {@code false} for the first registration.
     */
    <C extends NxCommand, R> boolean register(Class<C> type, CommandHandler<C, R> handler) {
        CommandTypeBinding binding = new CommandTypeBinding(type, handler);
        CommandTypeBinding previous = bindingsByMessageType.put(type.getSimpleName(), binding);
        return previous != null;
    }

    /**
     * Lookup a binding by {@code Nx-Message-Type} header value. Returns
     * {@code null} when the type has no registered handler.
     */
    @Nullable
    CommandTypeBinding lookup(String messageType) {
        if (messageType == null) {
            return null;
        }
        return bindingsByMessageType.get(messageType);
    }

    /**
     * Snapshot of registered class simple names, sorted for stable heartbeat
     * output. Used by {@link CommandsConsumer#currentStats()} for the
     * {@code registered-types} stats slot.
     */
    List<String> snapshotRegisteredTypes() {
        if (bindingsByMessageType.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<String>(bindingsByMessageType.keySet());
        Collections.sort(names);
        return Collections.unmodifiableList(names);
    }
}
