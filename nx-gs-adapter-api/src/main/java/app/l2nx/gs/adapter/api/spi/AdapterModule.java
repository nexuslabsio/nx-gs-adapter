package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStates;
import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStatus;

/**
 * Pluggable module discovered via {@link java.util.ServiceLoader} (public class, public no-arg constructor).
 * Connect: every {@code onConnect} in name-sorted order, then every {@code start}.
 * Shutdown: reverse order, every {@code stop} then every {@code onDisconnect}.
 * A {@link Throwable} from any hook is contained: the module goes {@code FAILED}, others continue.
 */
public interface AdapterModule {

    /** Unique across the classpath; duplicates collide under one heartbeat key. */
    String name();

    /** Wire resources here; work starts in {@link #start()}, after all modules have connected. */
    void onConnect(ConnectContext ctx);

    /** Start daemon work (schedulers, consumers). */
    void start();

    /** Must tolerate being called when {@code start()} never ran (module failed in {@code onConnect}). */
    void stop();

    /** Release shared resources; idempotent. */
    void onDisconnect();

    /**
     * Health snapshot folded into the heartbeat each tick; default is ACTIVE with empty stats.
     * If this throws, adapter-core reports FAILED.
     */
    default ModuleStatus currentStatus() {
        return ModuleStatus.builder()
                .name(name())
                .state(ModuleStates.ACTIVE)
                .stats(ModuleStatus.Stats.empty())
                .build();
    }
}
