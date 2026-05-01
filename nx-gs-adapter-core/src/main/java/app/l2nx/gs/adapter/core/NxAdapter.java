package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.Topics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.ConfigResolver;
import app.l2nx.gs.adapter.core.connect.ConnectFlow;
import app.l2nx.gs.adapter.core.connect.DefaultBackoffSchedule;
import app.l2nx.gs.adapter.core.connect.HttpURLConnectionConnectClient;
import app.l2nx.gs.adapter.core.heartbeat.HeartbeatService;
import app.l2nx.gs.adapter.core.kafka.DefaultKafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.adapter.core.lifecycle.AdapterVersion;
import app.l2nx.gs.adapter.core.lifecycle.StartupBanner;
import app.l2nx.gs.adapter.core.modules.ModuleRegistry;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private static final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * Latches on the first {@link AdapterState#ACTIVE}. Pre-latch a transient connect
     * failure stays in {@code REGISTERING}; post-latch it drives {@code DEGRADED}.
     */
    private static final AtomicBoolean wasActive = new AtomicBoolean(false);
    private static final Object transitionLock = new Object();
    private static volatile Consumer<AdapterState> stateCallback;
    private static final NxAdapter INSTANCE = new NxAdapter();

    private static volatile ScheduledExecutorService connectScheduler;
    private static volatile ScheduledExecutorService heartbeatScheduler;
    private static volatile HeartbeatService heartbeatService;
    private static volatile ModuleRegistry moduleRegistry;
    private static volatile String adapterVersion;
    private static volatile Thread shutdownHook;
    private static volatile KafkaFactory kafkaFactoryOverride;

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
        // Banner runs before config resolve so a misconfigured / disabled adapter still
        // announces itself.
        StartupBanner.emit(log, AdapterVersion.resolve());

        AdapterConfig config;
        try {
            config = new ConfigResolver().resolve();
        } catch (Throwable t) {
            log.error("Adapter failed to start due to config error: {}", t.getMessage(), t);
            transition(AdapterState.FAILED);
            return INSTANCE;
        }

        if (!config.isEnabled()) {
            log.info("L2NX adapter is disabled (l2nx.enabled=false) — set l2nx.enabled=true to activate");
            transition(AdapterState.DISABLED);
            return INSTANCE;
        }

        ScheduledExecutorService scheduler = createConnectScheduler();
        connectScheduler = scheduler;
        ScheduledExecutorService hbScheduler = createHeartbeatScheduler();
        heartbeatScheduler = hbScheduler;
        adapterVersion = config.getAdapterVersion();
        ModuleRegistry registry = new ModuleRegistry();
        moduleRegistry = registry;
        heartbeatService = new HeartbeatService(
                defaultPublisher(), hbScheduler, config.getAdapterVersion(), registry::currentStatuses);

        // Single read of the volatile so a concurrent test-only swap can't split
        // the null-check from the dereference.
        KafkaFactory override = kafkaFactoryOverride;
        KafkaFactory factory = override != null ? override : new DefaultKafkaFactory();
        KafkaInitializer kafkaInit = new KafkaInitializer(factory, config.getKafkaProducerOverrides());
        Consumer<ConnectResponse> onActive = response -> initKafka(kafkaInit, response);
        ConnectFlow flow = new ConnectFlow(
                config,
                new HttpURLConnectionConnectClient(),
                new DefaultBackoffSchedule(),
                scheduler,
                NxAdapter::handleConnectOutcome,
                onActive);
        scheduler.submit(SafeRunnable.wrap(flow, log));

        // Hook is registered last so FAILED / DISABLED paths never leave one attached.
        registerShutdownHook();
        return INSTANCE;
    }

    /**
     * Idempotent shutdown — cancel heartbeat, cancel connect scheduler, shut down the
     * Kafka client if alive, and transition to {@link AdapterState#CLOSED}. Safe to call
     * from any thread; safe to call repeatedly.
     */
    public void shutdown() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        HeartbeatService hb = heartbeatService;
        if (hb != null) {
            try {
                hb.stop();
            } catch (Throwable t) {
                log.error("HeartbeatService.stop threw: {}", t.getMessage(), t);
            }
            heartbeatService = null;
        }
        ScheduledExecutorService hbExec = heartbeatScheduler;
        if (hbExec != null) {
            // No awaitTermination — a late tick that races past cancel is absorbed by
            // defaultPublisher's KafkaException catch and HeartbeatService.tick's Throwable catch.
            hbExec.shutdownNow();
            heartbeatScheduler = null;
        }
        ScheduledExecutorService connect = connectScheduler;
        if (connect != null) {
            connect.shutdownNow();
            connectScheduler = null;
        }

        ModuleRegistry registry = moduleRegistry;
        if (registry != null) {
            try {
                registry.shutdown();
            } catch (Throwable t) {
                log.error("ModuleRegistry.shutdown threw {}", t.getClass().getName());
            }
            moduleRegistry = null;
        }

        try {
            NxKafka kafka = NxKafka.instance();
            if (kafka.state() != KafkaState.CLOSED) {
                kafka.shutdown();
            }
        } catch (KafkaException notConfigured) {
            // Adapter never reached initKafka — nothing to shut down.
        } catch (Throwable t) {
            log.error("NxKafka.shutdown threw {}", t.getClass().getName());
        }

        transition(AdapterState.CLOSED);
    }

    private static ScheduledExecutorService createConnectScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nx-adapter-connect");
            t.setDaemon(true);
            return t;
        });
    }

    private static ScheduledExecutorService createHeartbeatScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nx-adapter-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    private static HeartbeatService.KafkaPublisher defaultPublisher() {
        return (topic, key, payload) -> {
            try {
                NxKafka.instance().send(topic, key, payload);
            } catch (KafkaException notConfigured) {
                // Should never fire — initKafka arms heartbeat only after build() returns.
                log.warn("Heartbeat publisher invoked before NxKafka was configured");
            }
        };
    }

    private static void registerShutdownHook() {
        Thread hook = new Thread(SafeRunnable.wrap(INSTANCE::shutdown, log), "nx-adapter-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            shutdownHook = hook;
        } catch (Throwable t) {
            // JVM already shutting down or host SecurityManager disallows hooks; the
            // adapter still runs, operators just won't get auto-shutdown on JVM exit.
            log.warn("Failed to register JVM shutdown hook: {}", t.getClass().getName());
        }
    }

    private static void handleConnectOutcome(ConnectFlow.Outcome outcome) {
        switch (outcome) {
            case STARTING:
                transition(AdapterState.REGISTERING);
                break;
            case ACTIVE:
                // Reached only when Kafka wiring is skipped (test-only); production goes
                // through initKafka via onActiveResponse and never emits bare ACTIVE here.
                wasActive.set(true);
                transition(AdapterState.ACTIVE);
                break;
            case TRANSIENT:
                if (wasActive.get()) {
                    transition(AdapterState.DEGRADED);
                }
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

    private static void initKafka(KafkaInitializer init, ConnectResponse response) {
        String clientId = "nx-gs-adapter-" + response.getTenantSlug() + "-" + response.getServerSlug();
        KafkaState postBuild;
        try {
            postBuild = init.init(response.getKafka(), clientId, NxAdapter::handleKafkaStateChange);
        } catch (Throwable t) {
            log.error("Kafka init failed — adapter degraded: {}", t.getMessage(), t);
            transition(AdapterState.DEGRADED);
            return;
        }

        // Re-arming on a re-handshake recaptures connectInstant, so uptime is session-scoped.
        HeartbeatService hb = heartbeatService;
        Topics topics = response.getKafka() != null ? response.getKafka().getTopics() : null;
        String heartbeatTopic = topics != null ? topics.getHeartbeat() : null;
        if (hb != null && heartbeatTopic != null && response.getServerId() != null) {
            try {
                String tenantId = response.getTenantId() != null ? response.getTenantId().toString() : null;
                hb.start(
                        tenantId,
                        response.getTenantSlug(),
                        response.getServerId().toString(),
                        response.getServerSlug(),
                        response.getServerName(),
                        heartbeatTopic);
            } catch (Throwable t) {
                log.error("HeartbeatService.start threw {}", t.getClass().getName());
            }
        }

        ModuleRegistry registry = moduleRegistry;
        if (registry != null) {
            try {
                registry.discover();
                ConnectContext ctx = ConnectContext.builder()
                        .tenantId(response.getTenantId())
                        .tenantSlug(response.getTenantSlug())
                        .serverId(response.getServerId())
                        .serverSlug(response.getServerSlug())
                        .serverName(response.getServerName())
                        .adapterVersion(adapterVersion)
                        .syncTopics(response.getSyncTopics())
                        .build();
                registry.connect(ctx);
            } catch (Throwable t) {
                log.error("ModuleRegistry connect threw {}", t.getClass().getName());
            }
        }

        if (postBuild == KafkaState.CONNECTED) {
            wasActive.set(true);
            transition(AdapterState.ACTIVE);
        } else {
            transition(AdapterState.DEGRADED);
        }
    }

    private static void handleKafkaStateChange(KafkaState newState) {
        if (newState == null) {
            return;
        }
        switch (newState) {
            case CONNECTED:
                wasActive.set(true);
                transition(AdapterState.ACTIVE);
                break;
            case DISCONNECTED:
                AdapterState current = STATE.get();
                // A late event must not resurrect a CLOSED / FAILED / REJECTED adapter.
                if (current == AdapterState.ACTIVE || current == AdapterState.DEGRADED) {
                    transition(AdapterState.DEGRADED);
                }
                break;
            case CLOSED:
                // Shutdown drives CLOSED itself; honoring this would race with that path.
                break;
            case CREATED:
            default:
                break;
        }
    }

    private static void transition(AdapterState target) {
        // Serializes state-set + callback dispatch so a callback that calls state()
        // always observes the value just emitted, not a concurrently-set newer one.
        synchronized (transitionLock) {
            STATE.set(target);
            Consumer<AdapterState> cb = stateCallback;
            if (cb == null) {
                return;
            }
            try {
                cb.accept(target);
            } catch (Throwable t) {
                log.error("onStateChange callback threw {} on transition to {}",
                        t.getClass().getName(), target);
            }
        }
    }

    static void setKafkaFactoryForTesting(KafkaFactory factory) {
        kafkaFactoryOverride = factory;
    }

    static void resetForTesting() {
        Thread hook = shutdownHook;
        if (hook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
            }
            shutdownHook = null;
        }
        HeartbeatService hb = heartbeatService;
        if (hb != null) {
            hb.stop();
            heartbeatService = null;
        }
        ScheduledExecutorService hbExec = heartbeatScheduler;
        if (hbExec != null) {
            hbExec.shutdownNow();
            try {
                hbExec.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            heartbeatScheduler = null;
        }
        ScheduledExecutorService scheduler = connectScheduler;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            connectScheduler = null;
        }
        moduleRegistry = null;
        adapterVersion = null;
        STATE.set(AdapterState.INIT);
        stateCallback = null;
        started.set(false);
        closed.set(false);
        wasActive.set(false);
        kafkaFactoryOverride = null;
    }

    static void simulateKafkaStateChangeForTesting(KafkaState newState) {
        handleKafkaStateChange(newState);
    }

    static void simulateConnectOutcomeForTesting(ConnectFlow.Outcome outcome) {
        handleConnectOutcome(outcome);
    }

    static void simulateInitKafkaForTesting(KafkaInitializer init, ConnectResponse response) {
        initKafka(init, response);
    }

    /**
     * Test seam — primes the module-registry slot without going through
     * {@link #start()}. Required by tests that drive {@link #simulateInitKafkaForTesting}
     * directly and want the ServiceLoader-based module discovery to actually
     * happen (otherwise initKafka skips the {@code registry != null} branch).
     */
    static void primeModuleRegistryForTesting() {
        moduleRegistry = new ModuleRegistry();
    }
}
