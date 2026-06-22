package app.l2nx.gs.runtime.sync;

import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeStateProvider;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import app.l2nx.gs.runtime.sync.engine.EngineConfig;
import app.l2nx.gs.runtime.sync.engine.EntityStatsTracker;
import app.l2nx.gs.runtime.sync.engine.RuntimeSyncEngine;
import app.l2nx.gs.runtime.sync.engine.publish.KafkaSender;
import app.l2nx.gs.runtime.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.runtime.sync.engine.publish.TopicResolver;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.Callback;

/**
 * Tier-1 module that runs the runtime-sync engine. Reads its inputs in order:
 *
 * <ol>
 *     <li>{@code ctx.syncTopics().runtime()} — empty/null → DISABLED + WARN.</li>
 *     <li>Tier-2 SPI {@link RuntimeStateProvider} via {@link ServiceLoader} —
 *     0 → DISABLED + WARN, &gt;1 → FAILED, 1 → cached.</li>
 * </ol>
 *
 * <p>Discovered via
 * {@code META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule}.</p>
 */
public final class RuntimeSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(RuntimeSyncModule.class);

    static final String NAME = "runtime-sync";

    static final String STATE_INIT = "INIT";
    static final String STATE_DISABLED = "DISABLED";
    static final String STATE_FAILED = "FAILED";
    static final String STATE_ACTIVE = "ACTIVE";

    private final Supplier<List<RuntimeStateProvider>> providerDiscoverer;
    private final Function<String, String> configSource;
    private final KafkaSender kafkaSender;

    private volatile String state = STATE_INIT;
    private volatile RuntimeStateProvider provider;
    private volatile ConnectContext context;
    private volatile EntityStatsTracker statsTracker;
    private volatile RuntimeSyncEngine engine;
    private volatile List<RuntimeEntityMapping<?>> cachedMappings;

    public RuntimeSyncModule() {
        this(RuntimeSyncModule::loadProviders, EngineConfig.productionChain(), RuntimeSyncModule::sendViaNxKafka);
    }

    RuntimeSyncModule(
            Supplier<List<RuntimeStateProvider>> providerDiscoverer,
            Function<String, String> configSource,
            KafkaSender kafkaSender) {
        this.providerDiscoverer = providerDiscoverer;
        this.configSource = configSource;
        this.kafkaSender = kafkaSender;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void onConnect(ConnectContext ctx) {
        this.context = ctx;
        if (ctx == null
                || ctx.getSyncTopics() == null
                || ctx.getSyncTopics().getRuntime() == null
                || ctx.getSyncTopics().getRuntime().isEmpty()) {
            log.warn("ConnectContext carries no runtime sync topics — runtime-sync DISABLED. "
                    + "Platform must publish per-entity topics under syncTopics.runtime in /connect "
                    + "for the engine to run.");
            state = STATE_DISABLED;
            return;
        }

        List<RuntimeStateProvider> providers = providerDiscoverer.get();
        if (providers.isEmpty()) {
            log.warn("No RuntimeStateProvider SPI registered — runtime-sync DISABLED. "
                    + "Register one via META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider "
                    + "to enable runtime sync.");
            state = STATE_DISABLED;
            return;
        }
        if (providers.size() > 1) {
            log.error(
                    "Multiple RuntimeStateProvider impls on classpath: [{}]. runtime-sync FAILED.",
                    classNamesOf(providers));
            state = STATE_FAILED;
            return;
        }
        RuntimeStateProvider resolved = providers.get(0);
        List<RuntimeEntityMapping<?>> mappings = snapshotMappings(resolved);
        log.info("RuntimeStateProvider resolved: schemaName={}, mappings={}", resolved.schemaName(), mappings.size());
        this.cachedMappings = mappings;
        this.provider = resolved;
        this.statsTracker = new EntityStatsTracker();
        state = STATE_ACTIVE;
    }

    @Override
    public void start() {
        if (STATE_DISABLED.equals(state) || STATE_FAILED.equals(state) || STATE_INIT.equals(state)) {
            return;
        }
        RuntimeStateProvider p = provider;
        ConnectContext ctx = context;
        EntityStatsTracker tracker = statsTracker;
        List<RuntimeEntityMapping<?>> mappings = cachedMappings;
        if (p == null || ctx == null || tracker == null || mappings == null) {
            log.error("runtime-sync.start: missing dependency (provider/context/tracker/mappings) — staying {}", state);
            return;
        }
        if (mappings.isEmpty()) {
            log.warn("RuntimeStateProvider {} returned no mappings — runtime-sync DISABLED.", p.schemaName());
            state = STATE_DISABLED;
            return;
        }
        EngineConfig config = EngineConfig.from(configSource);
        TopicResolver resolver = TopicResolver.fromContext(ctx);
        SyncEventPublisher publisher = new SyncEventPublisher(kafkaSender);

        RuntimeSyncEngine built = new RuntimeSyncEngine(mappings, resolver, publisher, tracker, config);
        this.engine = built;
        try {
            built.start();
        } catch (Throwable t) {
            log.error(
                    "RuntimeSyncEngine.start threw {}: {} — runtime-sync FAILED",
                    t.getClass().getName(),
                    t.getMessage());
            state = STATE_FAILED;
            this.engine = null;
        }
    }

    @Override
    public void stop() {
        RuntimeSyncEngine running = engine;
        if (running != null) {
            try {
                running.stop();
            } catch (Throwable t) {
                log.error("RuntimeSyncEngine.stop threw {}", t.getClass().getName());
            }
            engine = null;
        }
    }

    @Override
    public void onDisconnect() {
        provider = null;
        context = null;
        statsTracker = null;
        engine = null;
        cachedMappings = null;
    }

    @Override
    public ModuleStatus currentStatus() {
        List<EntityStats> entityStats = null;
        EntityStatsTracker tracker = statsTracker;
        if (tracker != null) {
            try {
                List<EntityStats> entities = tracker.currentStatuses();
                if (entities != null && !entities.isEmpty()) {
                    entityStats = entities;
                }
            } catch (Throwable t) {
                log.warn(
                        "EntityStatsTracker.currentStatuses threw {}",
                        t.getClass().getName());
            }
        }
        ModuleStatus.Stats stats;
        if (entityStats == null) {
            stats = ModuleStatus.Stats.empty();
        } else {
            stats = ModuleStatus.Stats.builder().entities(entityStats).build();
        }
        return ModuleStatus.builder().name(NAME).state(state).stats(stats).build();
    }

    private static List<RuntimeEntityMapping<?>> snapshotMappings(RuntimeStateProvider provider) {
        List<? extends RuntimeEntityMapping<?>> raw = provider.mappings();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        List<RuntimeEntityMapping<?>> copy = new ArrayList<RuntimeEntityMapping<?>>(raw.size());
        for (RuntimeEntityMapping<?> m : raw) {
            copy.add(m);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<RuntimeStateProvider> loadProviders() {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(RuntimeSyncModule.class.getClassLoader());
            ServiceLoader<RuntimeStateProvider> loader = ServiceLoader.load(RuntimeStateProvider.class);
            List<RuntimeStateProvider> result = new ArrayList<RuntimeStateProvider>();
            for (RuntimeStateProvider item : loader) {
                result.add(item);
            }
            return Collections.unmodifiableList(result);
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    private static void sendViaNxKafka(String topic, byte[] key, Object value, Callback callback) {
        try {
            NxKafka.instance().send(topic, key, value, callback);
        } catch (Throwable senderFailure) {
            log.warn(
                    "NxKafka send threw {} — runtime-sync send dropped (topic={})",
                    senderFailure.getClass().getName(),
                    topic);
            try {
                callback.onCompletion(
                        null,
                        senderFailure instanceof Exception
                                ? (Exception) senderFailure
                                : new RuntimeException(senderFailure));
            } catch (Throwable t) {
                log.warn("KafkaSender callback threw {}", t.getClass().getName());
            }
        }
    }

    private static String classNamesOf(List<?> impls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < impls.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(impls.get(i).getClass().getName());
        }
        return sb.toString();
    }

    String stateForTesting() {
        return state;
    }

    Optional<EntityStatsTracker> statsTrackerForTesting() {
        return Optional.ofNullable(statsTracker);
    }
}
