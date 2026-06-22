package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.*;
import app.l2nx.gs.adapter.core.commands.CommandsBootstrap;
import app.l2nx.gs.adapter.core.commands.CommandsConfig;
import app.l2nx.gs.adapter.core.commands.CommandsConsumer;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.ConfigResolver;
import app.l2nx.gs.adapter.core.connect.*;
import app.l2nx.gs.adapter.core.events.EventsBootstrap;
import app.l2nx.gs.adapter.core.events.EventsConfig;
import app.l2nx.gs.adapter.core.events.EventsPublisher;
import app.l2nx.gs.adapter.core.gamedata.NxGameDataImpl;
import app.l2nx.gs.adapter.core.heartbeat.HeartbeatService;
import app.l2nx.gs.adapter.core.kafka.DefaultKafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.adapter.core.lifecycle.AdapterVersion;
import app.l2nx.gs.adapter.core.lifecycle.StartupBanner;
import app.l2nx.gs.adapter.core.modules.ModuleRegistry;
import app.l2nx.gs.adapter.core.sync.NxSyncImpl;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.KafkaState;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.*;
import java.util.concurrent.*;
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
    private static final Object shutdownLock = new Object();
    private static volatile Consumer<AdapterState> stateCallback;
    private static final NxAdapter INSTANCE = new NxAdapter();
    private static volatile ScheduledExecutorService connectScheduler;
    private static volatile ScheduledExecutorService heartbeatScheduler;
    private static volatile HeartbeatService heartbeatService;
    private static volatile ModuleRegistry moduleRegistry;
    private static volatile String adapterVersion;
    private static volatile Thread shutdownHook;
    private static volatile KafkaFactory kafkaFactoryOverride;
    private static volatile boolean failEventsBootstrapForTesting;
    private static volatile EventsPublisher eventsPublisher;
    private static volatile EventsConfig eventsConfig;
    private static volatile CommandsConsumer commandsConsumer;
    private static volatile CommandsConfig commandsConfig;
    private static volatile ExecutorService ioExecutor;
    private static volatile NxEvents eventsFacade;
    private static volatile NxCommands commandsFacade;
    private static volatile NxSyncImpl syncFacade;
    private static volatile NxGameDataImpl gameDataFacade;
    private static final AtomicReference<Executor> hostExecutorRef = new AtomicReference<Executor>();

    private NxAdapter() {}

    /**
     * Register a state-transition callback. May be called before or after {@link #start()}.
     * Replaces any previously registered callback.
     */
    public static void onStateChange(Consumer<AdapterState> callback) {
        stateCallback = callback;
    }

    /**
     * Register the host's game-side {@link Executor} for command-handler
     * {@code ctx.host().sync(...)} / {@code .async(...)} hops. MUST be called
     * before {@link #start()} when {@code commandsTopic} is configured.
     *
     * <p>Typical host wiring:</p>
     * <pre>
     *   NxAdapter.hostExecutor(task -&gt; ThreadPoolManager.getInstance().executeGeneral(task));
     *   NxAdapter.start();
     * </pre>
     *
     * <p>Calling without an executor when {@code commandsTopic} is configured
     * surfaces as a startup WARN; the first {@code ctx.host().sync(...)} call
     * from any handler then throws {@link IllegalStateException}. Read-only
     * handlers (no game state mutation) keep working unaffected.</p>
     *
     * <p>May be called more than once — last write wins. Replacing while the
     * adapter is already running is permitted but discouraged; the new
     * executor takes effect on the next {@code ctx.host()} call.</p>
     */
    public static void hostExecutor(Executor executor) {
        hostExecutorRef.set(executor);
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
            log.warn(
                    "NxAdapter.start() called more than once — ignoring duplicate invocation (current state: {})",
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
        if (ioExecutor == null) {
            ioExecutor = createIoExecutor(config.getIoWorkers());
        }
        adapterVersion = config.getAdapterVersion();
        eventsConfig = config.getEvents();
        commandsConfig = config.getCommands();
        ModuleRegistry registry = new ModuleRegistry();
        moduleRegistry = registry;
        heartbeatService = new HeartbeatService(
                defaultPublisher(), hbScheduler, config.getAdapterVersion(), NxAdapter::collectModuleStatuses);

        // Single read of the volatile so a concurrent test-only swap can't split
        // the null-check from the dereference.
        KafkaFactory override = kafkaFactoryOverride;
        KafkaFactory factory = override != null ? override : new DefaultKafkaFactory();
        KafkaInitializer kafkaInit = new KafkaInitializer(factory, config.getKafkaProducerOverrides());
        HostConnectFlow<?> hostFlow = "ls".equals(config.getHostType())
                ? new LoginServerConnectFlow(config)
                : new GameServerConnectFlow(config);
        Consumer<HostConnectFlow<?>> onActive = active -> initKafka(kafkaInit, active);
        ConnectFlow flow = new ConnectFlow(
                hostFlow, new DefaultBackoffSchedule(), scheduler, NxAdapter::handleConnectOutcome, onActive);
        scheduler.submit(SafeRunnable.wrap(flow, log));

        // Hook is registered last so FAILED / DISABLED paths never leave one attached.
        registerShutdownHook();
        return INSTANCE;
    }

    /**
     * Idempotent shutdown — cancel heartbeat, cancel connect scheduler, shut down the
     * Kafka client if alive, and transition to {@link AdapterState#CLOSED}. Safe to call
     * from any thread; safe to call repeatedly.
     *
     * <p>Blocking-idempotent: a concurrent second caller does not return early — it
     * waits until the in-flight shutdown completes. This lets a host drive shutdown
     * synchronously before a hard {@code Runtime.halt()} (which skips the
     * auto-registered JVM hook) and rely on the event queue being drained and the
     * Kafka producer flushed by the time this returns, even if the auto hook is
     * running the shutdown on another thread.</p>
     */
    public void shutdown() {
        synchronized (shutdownLock) {
            shutdownLocked();
        }
    }

    private void shutdownLocked() {
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
            // Let an in-flight tick complete its publish rather than interrupting
            // it mid-send (shutdownNow would surface as a noisy WakeupException).
            hbExec.shutdown();
            try {
                if (!hbExec.awaitTermination(2L, TimeUnit.SECONDS)) {
                    hbExec.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                hbExec.shutdownNow();
            }
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
                log.error("ModuleRegistry.shutdown threw {}", t.getClass().getName(), t);
            }
            moduleRegistry = null;
        }

        // Stop the commands consumer first so the daemon stops admitting new
        // records and in-flight handlers can finish + emit their replies before
        // we tear down the producer that NxKafka owns.
        CommandsConsumer cmds = commandsConsumer;
        if (cmds != null) {
            try {
                cmds.stop();
            } catch (Throwable t) {
                log.error("CommandsConsumer.stop threw {}", t.getClass().getName(), t);
            }
            commandsConsumer = null;
        }

        ExecutorService io = ioExecutor;
        if (io != null) {
            io.shutdown();
            try {
                if (!io.awaitTermination(5L, TimeUnit.SECONDS)) {
                    io.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                io.shutdownNow();
            }
            ioExecutor = null;
        }

        EventsPublisher pub = eventsPublisher;
        if (pub != null) {
            try {
                pub.stop();
            } catch (Throwable t) {
                log.error("EventsPublisher.stop threw {}", t.getClass().getName(), t);
            }
            eventsPublisher = null;
        }

        try {
            NxKafka kafka = NxKafka.instance();
            if (kafka.state() != KafkaState.CLOSED) {
                kafka.shutdown();
            }
        } catch (KafkaException notConfigured) {
            // Adapter never reached initKafka — nothing to shut down.
        } catch (Throwable t) {
            log.error("NxKafka.shutdown threw {}", t.getClass().getName(), t);
        }

        transition(AdapterState.CLOSED);
    }

    private static ScheduledExecutorService createConnectScheduler() {
        return Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.named("nx-adapter-connect", log));
    }

    private static ScheduledExecutorService createHeartbeatScheduler() {
        return Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.named("nx-adapter-heartbeat", log));
    }

    private static ExecutorService createIoExecutor(int workers) {
        return Executors.newFixedThreadPool(workers, DaemonThreadFactory.counted("nx-io-", log));
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
            log.warn("Failed to register JVM shutdown hook: {}", t.getClass().getName(), t);
        }
    }

    private static void handleConnectOutcome(ConnectFlow.Outcome outcome) {
        switch (outcome) {
            case STARTING:
                transition(AdapterState.REGISTERING);
                break;
            case ACTIVE:
                // Reached only when Kafka wiring is skipped (test-only); production goes
                // through initKafka via onActiveFlow and never emits bare ACTIVE here.
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

    private static void initKafka(KafkaInitializer init, HostConnectFlow<?> active) {
        // Test paths can drive initKafka directly without going through start();
        // prime a small IO pool so ConnectContext.io() and CommandContext.io() are usable.
        if (ioExecutor == null) {
            ioExecutor = createIoExecutor(AdapterConfig.defaultIoWorkers());
        }
        String clientId = "nx-gs-adapter-" + active.tenantSlug() + "-" + active.serverSlug();
        Map<String, byte[]> staticHeaders = buildStaticHeaders(active.serverId());
        KafkaState postBuild;
        try {
            postBuild = init.init(active.kafka(), clientId, staticHeaders, NxAdapter::handleKafkaStateChange);
        } catch (Throwable t) {
            log.error("Kafka init failed — adapter degraded: {}", t.getMessage(), t);
            transition(AdapterState.DEGRADED);
            return;
        }

        // Re-arming on a re-handshake recaptures connectInstant, so uptime is session-scoped.
        HeartbeatService hb = heartbeatService;
        String heartbeatTopic = active.heartbeatTopic();
        if (hb != null && heartbeatTopic != null && active.serverId() != null) {
            try {
                String tenantId = active.tenantId() != null ? active.tenantId().toString() : null;
                hb.start(
                        tenantId,
                        active.tenantSlug(),
                        active.serverId().toString(),
                        active.serverSlug(),
                        active.serverName(),
                        heartbeatTopic);
            } catch (Throwable t) {
                log.error("HeartbeatService.start threw {}", t.getClass().getName(), t);
            }
        }

        // Events and commands bootstrap are isolated: a failure in either (e.g. a
        // NoClassDefFoundError from an adapter-api/adapter-core version skew on the
        // host classpath) must NOT abort sync-module discovery below, which is the
        // adapter's primary job. ConnectContext null-coalesces a missing façade to a
        // no-op, and the heartbeat surfaces the gap by omitting the events/commands slot.
        NxEvents events = null;
        try {
            events = startEventsPublisher(active.topics());
        } catch (Throwable t) {
            log.error(
                    "Events publisher bootstrap failed — events disabled, continuing with sync/commands: {}",
                    t.getClass().getName(),
                    t);
        }
        // Group ID lives under the per-tenant prefix so the `User:<tenant>` SCRAM
        // principal's group ACL (prefixed on `<tenant>.`) covers it.
        String commandsGroupId = active.tenantSlug() + ".gs.commands." + active.serverSlug();
        NxSync sync = startSyncFacade();
        NxGameData gameData = startGameDataFacade();
        NxCommands commands = null;
        try {
            commands = startCommandsConsumer(
                    active.topics(), active.kafka(), clientId, commandsGroupId, active.serverId(), events, sync);
        } catch (Throwable t) {
            log.error(
                    "Commands consumer bootstrap failed — commands disabled, continuing with sync: {}",
                    t.getClass().getName(),
                    t);
        }

        ModuleRegistry registry = moduleRegistry;
        if (registry != null) {
            try {
                registry.discover();
                ConnectContext ctx = ConnectContext.builder()
                        .tenantId(active.tenantId())
                        .tenantSlug(active.tenantSlug())
                        .serverId(active.serverId())
                        .serverSlug(active.serverSlug())
                        .serverName(active.serverName())
                        .adapterVersion(adapterVersion)
                        .syncTopics(active.syncTopics())
                        .events(events)
                        .commands(commands)
                        .io(ioExecutor)
                        .sync(sync)
                        .gameData(gameData)
                        .build();
                registry.connect(ctx);
            } catch (Throwable t) {
                log.error("ModuleRegistry connect threw {}", t.getClass().getName(), t);
            }
        }

        if (postBuild == KafkaState.CONNECTED) {
            wasActive.set(true);
            transition(AdapterState.ACTIVE);
        } else {
            transition(AdapterState.DEGRADED);
        }
    }

    private static Map<String, byte[]> buildStaticHeaders(UUID serverId) {
        if (serverId == null) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(NxHeaders.NX_SERVER_ID, NxHeaders.encodeUuid(serverId));
    }

    /**
     * Build and start the events publisher with the per-family topic map from
     * the connect response. Returns the {@link NxEvents} façade that goes
     * into {@link ConnectContext#events()}. {@code messagingTopics}
     * absent on the wire is normalized to an empty event-family map — the
     * publisher still spins up so the heartbeat slot is populated; every
     * {@code publishX} call short-circuits as a no-op + DEBUG log without
     * burning queue capacity.
     *
     * <p>The façade is stable across reconnects — on a second handshake the
     * underlying publisher is swapped behind the same {@link NxEvents}
     * instance so modules that captured {@code ctx.events()} earlier keep
     * publishing into the live publisher with no re-binding.</p>
     */
    private static NxEvents startEventsPublisher(MessagingTopics messagingTopics) {
        if (failEventsBootstrapForTesting) {
            throw new NoClassDefFoundError(
                    "app/l2nx/gs/adapter/api/kafka/events/raid/RaidKillEvent (simulated classpath skew)");
        }
        EventsPublisher previous = eventsPublisher;
        if (previous != null) {
            try {
                previous.stop();
            } catch (Throwable t) {
                log.error(
                        "EventsPublisher.stop on reconnect threw {}",
                        t.getClass().getName(),
                        t);
            }
        }
        Map<String, String> familyTopics =
                messagingTopics != null ? messagingTopics.getEvents() : Collections.emptyMap();
        EventsConfig cfg = eventsConfig != null ? eventsConfig : EventsConfig.defaults();
        EventsPublisher.Sender sender = (record, callback) -> NxKafka.instance().sendBytesKeyRecord(record, callback);
        EventsPublisher.ProducerFlusher flusher = () -> NxKafka.instance().flush();
        NxEvents facade = eventsFacade;
        if (facade == null) {
            EventsBootstrap.Started started = EventsBootstrap.start(familyTopics, sender, flusher, cfg);
            eventsPublisher = started.publisher();
            eventsFacade = started.events();
            return eventsFacade;
        }
        eventsPublisher = EventsBootstrap.swap(facade, familyTopics, sender, flusher, cfg);
        return facade;
    }

    /**
     * Build and start the commands consumer with the inbound + replies topic
     * pair from the connect response. Returns the {@link NxCommands} façade
     * that goes into {@link ConnectContext#commands()} — non-null even when
     * commands are disabled (registrations are accepted as no-ops). When
     * {@code commandsTopic} is unconfigured no consumer thread is spawned.
     */
    private static NxCommands startCommandsConsumer(
            MessagingTopics messagingTopics,
            KafkaCredentials kafka,
            String clientId,
            String groupId,
            UUID ownServerId,
            NxEvents events,
            NxSync sync) {
        CommandsConsumer previous = commandsConsumer;
        if (previous != null) {
            try {
                previous.stop();
            } catch (Throwable t) {
                log.error(
                        "CommandsConsumer.stop on reconnect threw {}",
                        t.getClass().getName(),
                        t);
            }
        }
        CommandsConsumer.ReplySender replySender =
                (record, callback) -> NxKafka.instance().sendBytesKeyRecord(record, callback);
        NxCommands facade = commandsFacade;
        if (facade == null) {
            CommandsBootstrap.Started started = CommandsBootstrap.start(
                    messagingTopics,
                    kafka,
                    clientId,
                    groupId,
                    ownServerId,
                    hostExecutorRef.get(),
                    ioExecutor,
                    events,
                    sync,
                    replySender,
                    commandsConfig);
            commandsConsumer = started.consumer();
            commandsFacade = started.commands();
            return commandsFacade;
        }
        commandsConsumer = CommandsBootstrap.swap(
                facade,
                messagingTopics,
                kafka,
                clientId,
                groupId,
                ownServerId,
                hostExecutorRef.get(),
                ioExecutor,
                events,
                sync,
                replySender,
                commandsConfig);
        return facade;
    }

    private static NxSync startSyncFacade() {
        NxSyncImpl existing = syncFacade;
        if (existing != null) {
            existing.clearTriggers();
            return existing;
        }
        NxSyncImpl fresh = new NxSyncImpl();
        syncFacade = fresh;
        return fresh;
    }

    private static NxGameData startGameDataFacade() {
        NxGameDataImpl existing = gameDataFacade;
        if (existing == null) {
            existing = new NxGameDataImpl();
            gameDataFacade = existing;
        } else {
            // Reconnect: drop stale triggers so the gd-sync module re-registers
            // cleanly instead of stacking duplicates.
            existing.clearTriggers();
        }
        existing.bindExecutor(ioExecutor);
        return existing;
    }

    /**
     * Heartbeat-supplier seam — combines registry-discovered modules with the
     * built-in {@code events} and {@code commands} module slots. Called once
     * per heartbeat tick on the heartbeat scheduler thread.
     */
    private static List<ModuleStatus> collectModuleStatuses() {
        ModuleRegistry registry = moduleRegistry;
        List<ModuleStatus> registryStatuses = registry != null ? registry.currentStatuses() : Collections.emptyList();
        EventsPublisher pub = eventsPublisher;
        CommandsConsumer cmds = commandsConsumer;
        int extra = (pub != null ? 1 : 0) + (cmds != null ? 1 : 0);
        if (extra == 0) {
            return registryStatuses != null ? registryStatuses : Collections.emptyList();
        }
        List<ModuleStatus> all =
                new ArrayList<ModuleStatus>((registryStatuses != null ? registryStatuses.size() : 0) + extra);
        if (registryStatuses != null) {
            all.addAll(registryStatuses);
        }
        if (pub != null) {
            try {
                all.add(pub.currentStatus());
            } catch (Throwable t) {
                log.error("EventsPublisher.currentStatus threw {}", t.getClass().getName(), t);
            }
        }
        if (cmds != null) {
            try {
                all.add(cmds.currentStatus());
            } catch (Throwable t) {
                log.error(
                        "CommandsConsumer.currentStatus threw {}", t.getClass().getName(), t);
            }
        }
        return all;
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
                log.error(
                        "onStateChange callback threw {} on transition to {}",
                        t.getClass().getName(),
                        target);
            }
        }
    }

    static void setKafkaFactoryForTesting(KafkaFactory factory) {
        kafkaFactoryOverride = factory;
    }

    /**
     * Test seam — installs an externally managed IO executor so tests
     * exercising {@link #simulateInitKafkaForTesting} skip the production
     * pool spin-up. The caller owns lifecycle.
     */
    static void setIoExecutorForTesting(ExecutorService executor) {
        ioExecutor = executor;
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
        CommandsConsumer cmds = commandsConsumer;
        if (cmds != null) {
            try {
                cmds.stop();
            } catch (Throwable t) {
                log.error(
                        "CommandsConsumer.stop in resetForTesting threw {}",
                        t.getClass().getName(),
                        t);
            }
            commandsConsumer = null;
        }
        EventsPublisher pub = eventsPublisher;
        if (pub != null) {
            try {
                pub.stop();
            } catch (Throwable t) {
                log.error(
                        "EventsPublisher.stop in resetForTesting threw {}",
                        t.getClass().getName(),
                        t);
            }
            eventsPublisher = null;
        }
        ExecutorService io = ioExecutor;
        if (io != null) {
            io.shutdownNow();
            try {
                io.awaitTermination(1L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ioExecutor = null;
        }
        eventsFacade = null;
        commandsFacade = null;
        NxSyncImpl syncImpl = syncFacade;
        if (syncImpl != null) {
            syncImpl.clearTriggers();
        }
        syncFacade = null;
        NxGameDataImpl gameDataImpl = gameDataFacade;
        if (gameDataImpl != null) {
            gameDataImpl.clearTriggers();
        }
        gameDataFacade = null;
        eventsConfig = null;
        commandsConfig = null;
        hostExecutorRef.set(null);
        adapterVersion = null;
        STATE.set(AdapterState.INIT);
        stateCallback = null;
        started.set(false);
        closed.set(false);
        wasActive.set(false);
        kafkaFactoryOverride = null;
        failEventsBootstrapForTesting = false;
    }

    static void failEventsBootstrapForTesting(boolean fail) {
        failEventsBootstrapForTesting = fail;
    }

    static void simulateKafkaStateChangeForTesting(KafkaState newState) {
        handleKafkaStateChange(newState);
    }

    static void simulateConnectOutcomeForTesting(ConnectFlow.Outcome outcome) {
        handleConnectOutcome(outcome);
    }

    static void simulateInitKafkaForTesting(KafkaInitializer init, ConnectResponse response) {
        initKafka(init, new GameServerResponseFlowAdapter(response));
    }

    /**
     * Test-only adapter wrapping a fully-built {@link ConnectResponse} as a
     * {@link HostConnectFlow}. Lets tests exercise the production
     * {@code initKafka(HostConnectFlow)} path without standing up a real
     * {@link GameServerConnectFlow} + WireMock.
     */
    private static final class GameServerResponseFlowAdapter implements HostConnectFlow<ConnectResponse> {
        private final ConnectResponse r;

        GameServerResponseFlowAdapter(ConnectResponse r) {
            this.r = r;
        }

        @Override
        public TypedConnectOutcome<ConnectResponse> connect() {
            return TypedConnectOutcome.ok(r);
        }

        @Override
        public String connectPath() {
            return GameServerConnectFlow.CONNECT_PATH;
        }

        @Override
        public ConnectResponse response() {
            return r;
        }

        @Override
        public String heartbeatTopic() {
            return r.getHeartbeatTopic();
        }

        @Override
        public MessagingTopics topics() {
            return r.getMessagingTopics();
        }

        @Override
        public SyncTopics syncTopics() {
            return r.getSyncTopics();
        }

        @Override
        public UUID serverId() {
            return r.getServerId();
        }

        @Override
        public UUID tenantId() {
            return r.getTenantId();
        }

        @Override
        public String tenantSlug() {
            return r.getTenantSlug();
        }

        @Override
        public String serverSlug() {
            return r.getServerSlug();
        }

        @Override
        public String serverName() {
            return r.getServerName();
        }

        @Override
        public KafkaCredentials kafka() {
            return r.getKafka();
        }
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
