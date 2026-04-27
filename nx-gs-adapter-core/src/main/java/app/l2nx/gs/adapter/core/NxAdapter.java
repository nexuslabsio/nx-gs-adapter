package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.ConfigResolver;
import app.l2nx.gs.adapter.core.connect.ConnectFlow;
import app.l2nx.gs.adapter.core.connect.DefaultBackoffSchedule;
import app.l2nx.gs.adapter.core.connect.HttpURLConnectionConnectClient;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Adapter entry point — singleton-style facade exposing the lifecycle to the host JVM.
 *
 * <p>Usage:</p>
 * <pre>
 *   NxAdapter.onStateChange(s -&gt; log.info("adapter state: {}", s)); // optional
 *   NxAdapter.start();                                                 // fire-and-forget
 * </pre>
 *
 * <p>{@link #start()} never propagates an exception to the host JVM. Any failure during
 * config resolution is caught here, logged via {@link NxLog}, and reflected in
 * {@link #state()} as {@link AdapterState#FAILED}.</p>
 */
public final class NxAdapter {

    private static final NxLog log = NxLogFactory.getLogger(NxAdapter.class);
    private static final AtomicReference<AdapterState> STATE = new AtomicReference<>(AdapterState.INIT);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final Object transitionLock = new Object();
    private static volatile Consumer<AdapterState> stateCallback;
    private static final NxAdapter INSTANCE = new NxAdapter();

    private static volatile ScheduledExecutorService connectScheduler;

    private NxAdapter() {
    }

    /**
     * Register a state-transition callback. May be called before or after {@link #start()}.
     * Replaces any previously registered callback.
     */
    public static void onStateChange(Consumer<AdapterState> callback) {
        stateCallback = callback;
    }

    /**
     * Current adapter state.
     */
    public static AdapterState state() {
        return STATE.get();
    }

    /**
     * Resolve config and (if enabled) initiate the connect flow on a daemon scheduler.
     * Non-blocking — returns immediately. Never throws into the host JVM.
     */
    public static NxAdapter start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("NxAdapter.start() called more than once — ignoring duplicate invocation (current state: {})",
                    STATE.get());
            return INSTANCE;
        }
        AdapterConfig config;
        try {
            config = new ConfigResolver().resolve();
        } catch (Throwable t) {
            log.error("Adapter failed to start due to config error: {}", t.getMessage(), t);
            transition(AdapterState.FAILED);
            return INSTANCE;
        }

        if (!config.isEnabled()) {
            // Full DISABLED-state semantics (INFO log + transition + no callbacks elsewhere)
            // land in M33. For now we simply skip wiring the connect flow so an adapter
            // configured with l2nx.enabled=false stays inert.
            return INSTANCE;
        }

        ScheduledExecutorService scheduler = createConnectScheduler();
        connectScheduler = scheduler;
        ConnectFlow flow = new ConnectFlow(
                config,
                new HttpURLConnectionConnectClient(),
                new DefaultBackoffSchedule(),
                scheduler,
                NxAdapter::handleConnectOutcome);
        scheduler.submit(flow);
        return INSTANCE;
    }

    /**
     * Idempotent shutdown — full implementation lands in M29.
     */
    public void shutdown() {
        // M29 will fill this in.
    }

    private static ScheduledExecutorService createConnectScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nx-adapter-connect");
            t.setDaemon(true);
            return t;
        });
    }

    private static void handleConnectOutcome(ConnectFlow.Outcome outcome) {
        switch (outcome) {
            case STARTING:
                transition(AdapterState.REGISTERING);
                break;
            case ACTIVE:
                transition(AdapterState.ACTIVE);
                break;
            case TRANSIENT:
                transition(AdapterState.DEGRADED);
                break;
            case FAILED:
                transition(AdapterState.FAILED);
                break;
            case REJECTED:
                transition(AdapterState.REJECTED);
                break;
            default:
                break;
        }
    }

    private static void transition(AdapterState target) {
        // Serialize state-set + callback dispatch so an observer that calls state()
        // from inside the callback always sees the value that was just emitted —
        // a concurrent transition cannot interleave between the set and the dispatch.
        // Callback runs under the lock; hosts that need async observer threads must
        // hand off inside the callback (per the adapter's "host owns thread-handoff"
        // contract).
        synchronized (transitionLock) {
            STATE.set(target);
            Consumer<AdapterState> cb = stateCallback;
            if (cb == null) {
                return;
            }
            try {
                cb.accept(target);
            } catch (Throwable t) {
                log.error("onStateChange callback threw on transition to {}: {}",
                        target, t.getMessage(), t);
            }
        }
    }

    // Visible for testing — clears static state and tears down any scheduler launched
    // by a previous test run. Safe to call from @BeforeEach / @AfterEach.
    static void resetForTesting() {
        ScheduledExecutorService scheduler = connectScheduler;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            connectScheduler = null;
        }
        STATE.set(AdapterState.INIT);
        stateCallback = null;
        started.set(false);
    }
}
