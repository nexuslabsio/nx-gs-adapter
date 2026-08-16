package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStates;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;

/**
 * Tier-1 SPI: pluggable module discovered by {@code nx-gs-adapter-core} via
 * {@link java.util.ServiceLoader} once at JVM bootstrap. Implementations ship a
 * descriptor at {@code META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule}
 * pointing to a public class with a public no-arg constructor.
 *
 * <p>Lifecycle dispatch is owned by adapter-core:</p>
 * <ol>
 *     <li><b>connect</b> — for every discovered module, in name-sorted order:
 *         {@link #onConnect(ConnectContext)}, then (after every module's onConnect
 *         finishes) a second pass invoking {@link #start()}.</li>
 *     <li><b>shutdown</b> — modules in <b>reverse</b> name-sorted order:
 *         {@link #stop()}, then (after every module's stop finishes) a second pass
 *         invoking {@link #onDisconnect()}.</li>
 * </ol>
 *
 * <p>Every hook invocation is wrapped in adapter-core's {@code SafeRunnable} —
 * a {@link Throwable} from any hook never reaches the host JVM thread; the throwing
 * module transitions to internal {@code FAILED} state and other modules continue.</p>
 */
public interface AdapterModule {

    /**
     * Unique module identifier, surfaced as {@link ModuleStatus#getName()}. Examples:
     * {@code "db-sync"}, {@code "dp-sync"}, future {@code "metrics"}. Two modules with
     * the same name on the classpath is a packaging bug — both will appear in the
     * heartbeat under the same key.
     */
    String name();

    /**
     * Called once after the platform handshake completes and the Kafka producer is
     * ready. Modules wire up their own resources here (resolve sub-tier SPIs, open
     * connections, build mappers). {@code start()} is invoked separately, in a second
     * pass, so cross-module references (rare) can be resolved here before any module
     * actually begins doing work.
     */
    void onConnect(ConnectContext ctx);

    /**
     * Called once after every module's {@link #onConnect(ConnectContext)} has
     * completed. Modules kick off their own daemon work here (schedulers, consumers,
     * background ticks).
     */
    void start();

    /**
     * Called on adapter shutdown. Modules cancel their schedulers, drain in-flight
     * work, but do not yet release shared resources. Idempotent — called at most once
     * per session, but the implementation MUST tolerate being called when {@code start()}
     * never ran (e.g. the module failed in {@code onConnect}).
     */
    void stop();

    /**
     * Called after every module's {@link #stop()} has completed. Modules release
     * connections, close pools, drop in-memory state. Idempotent.
     */
    void onDisconnect();

    /**
     * Snapshot of the module's current health for heartbeat enrichment. Adapter-core
     * invokes this per-tick (wrapped in {@code SafeRunnable}) and folds the result
     * into {@code HeartbeatEvent.enabledModules}.
     *
     * <p>Default returns {@code {name, ModuleStates.ACTIVE, empty Stats}}. Modules override
     * to report degraded states or attach module-specific stats (e.g. db-sync injects
     * the {@code pool} slot from its {@code JdbcConnectionSource.stats()}). If this
     * method throws, adapter-core falls back to {@code {name, ModuleStates.FAILED, empty Stats}}.</p>
     */
    default ModuleStatus currentStatus() {
        return ModuleStatus.builder()
                .name(name())
                .state(ModuleStates.ACTIVE)
                .stats(ModuleStatus.Stats.empty())
                .build();
    }
}
