package app.l2nx.gs.db.sync;

import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.DbSchemaProvider;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.CdcEngine;
import app.l2nx.gs.db.sync.engine.EngineConfig;
import app.l2nx.gs.db.sync.engine.EntityStatsTracker;
import app.l2nx.gs.db.sync.engine.SnapshotStore;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Tier-1 module that runs the CRC32 CDC engine. Reads its inputs in order:
 *
 * <ol>
 *     <li>{@code ctx.syncTopics()} — empty/null → DISABLED + WARN.</li>
 *     <li>Tier-3 SPI {@link JdbcConnectionSource} via {@link ServiceLoader} —
 *     0 → FAILED, &gt;1 → FAILED, 1 → cached + smoke-checked. Smoke check
 *     failure → DEGRADED but engine still runs (so the next cycle retries).</li>
 *     <li>Tier-2 SPI {@link DbSchemaProvider} via {@link ServiceLoader} —
 *     0 → DISABLED + WARN, &gt;1 → FAILED, 1 → cached.</li>
 * </ol>
 *
 * <p>State surfaces in {@link #currentStatus()} for heartbeat enrichment.</p>
 *
 * <p>Discovered via
 * {@code META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule} —
 * the public no-arg constructor is what {@link ServiceLoader} invokes.</p>
 */
public final class DbSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(DbSyncModule.class);

    static final String NAME = "db-sync";
    static final int SMOKE_VALID_TIMEOUT_SECONDS = 5;

    static final String STATE_INIT = "INIT";
    static final String STATE_DISABLED = "DISABLED";
    static final String STATE_FAILED = "FAILED";
    static final String STATE_DEGRADED = "DEGRADED";
    static final String STATE_ACTIVE = "ACTIVE";

    private final Supplier<List<JdbcConnectionSource>> jdbcDiscoverer;
    private final Supplier<List<DbSchemaProvider>> schemaDiscoverer;
    private final Predicate<JdbcConnectionSource> smokeChecker;
    private final Function<String, String> configSource;
    private final KafkaSender kafkaSender;

    private volatile String state = STATE_INIT;
    private volatile JdbcConnectionSource source;
    private volatile DbSchemaProvider provider;
    private volatile ConnectContext context;
    private volatile EntityStatsTracker statsTracker;
    private volatile CdcEngine engine;

    public DbSyncModule() {
        this(DbSyncModule::loadJdbc,
                DbSyncModule::loadSchema,
                DbSyncModule::performSmokeCheck,
                EngineConfig.productionChain(),
                DbSyncModule::sendViaNxKafka);
    }

    DbSyncModule(Supplier<List<JdbcConnectionSource>> jdbcDiscoverer,
                 Supplier<List<DbSchemaProvider>> schemaDiscoverer,
                 Predicate<JdbcConnectionSource> smokeChecker,
                 Function<String, String> configSource,
                 KafkaSender kafkaSender) {
        this.jdbcDiscoverer = jdbcDiscoverer;
        this.schemaDiscoverer = schemaDiscoverer;
        this.smokeChecker = smokeChecker;
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
        if (ctx == null || ctx.getSyncTopics() == null || ctx.getSyncTopics().isEmpty()) {
            log.warn("ConnectContext carries no syncTopics — db-sync DISABLED. "
                    + "Platform must publish per-entity topics in /connect for the engine to run.");
            state = STATE_DISABLED;
            return;
        }

        List<JdbcConnectionSource> jdbcImpls = jdbcDiscoverer.get();
        if (jdbcImpls.isEmpty()) {
            log.error("No JdbcConnectionSource SPI registered — register one via "
                    + "META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource. "
                    + "db-sync FAILED.");
            state = STATE_FAILED;
            return;
        }
        if (jdbcImpls.size() > 1) {
            log.error("Multiple JdbcConnectionSource impls on classpath: [{}]. db-sync FAILED.",
                    namesOf(jdbcImpls));
            state = STATE_FAILED;
            return;
        }
        JdbcConnectionSource jdbc = jdbcImpls.get(0);
        log.info("JdbcConnectionSource resolved: {}", jdbc.name());
        boolean smokeOk = smokeChecker.test(jdbc);
        this.source = jdbc;

        List<DbSchemaProvider> schemaImpls = schemaDiscoverer.get();
        if (schemaImpls.isEmpty()) {
            log.warn("No DbSchemaProvider SPI registered — db-sync DISABLED. "
                    + "Register one via META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider "
                    + "to enable CDC sync.");
            state = STATE_DISABLED;
            return;
        }
        if (schemaImpls.size() > 1) {
            log.error("Multiple DbSchemaProvider impls on classpath: [{}]. db-sync FAILED.",
                    classNamesOf(schemaImpls));
            state = STATE_FAILED;
            return;
        }
        DbSchemaProvider resolvedProvider = schemaImpls.get(0);
        log.info("DbSchemaProvider resolved: schemaName={}, mappings={}",
                resolvedProvider.schemaName(),
                resolvedProvider.mappings() == null ? 0 : resolvedProvider.mappings().size());
        this.provider = resolvedProvider;
        this.statsTracker = new EntityStatsTracker();

        state = smokeOk ? STATE_ACTIVE : STATE_DEGRADED;
    }

    @Override
    public void start() {
        if (STATE_DISABLED.equals(state) || STATE_FAILED.equals(state) || STATE_INIT.equals(state)) {
            return;
        }
        DbSchemaProvider p = provider;
        JdbcConnectionSource s = source;
        ConnectContext ctx = context;
        EntityStatsTracker tracker = statsTracker;
        if (p == null || s == null || ctx == null || tracker == null) {
            log.error("db-sync.start: missing dependency (provider/source/context/tracker) — staying {}", state);
            return;
        }
        List<? extends app.l2nx.gs.adapter.api.spi.EntityMapping<?>> mappings = p.mappings();
        if (mappings == null || mappings.isEmpty()) {
            log.warn("DbSchemaProvider {} returned no mappings — db-sync DISABLED.", p.schemaName());
            state = STATE_DISABLED;
            return;
        }
        EngineConfig config = EngineConfig.from(configSource);
        TopicResolver resolver = TopicResolver.fromContext(ctx);
        SyncEventPublisher publisher = new SyncEventPublisher(kafkaSender);

        CdcEngine built = new CdcEngine(
                p.schemaName(),
                mappings,
                s,
                new SnapshotStore(),
                config,
                resolver,
                publisher,
                tracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                configSource);
        this.engine = built;
        try {
            built.start();
            // Engine started — preserve any DEGRADED/ACTIVE state set by onConnect.
        } catch (Throwable t) {
            log.error("CdcEngine.start threw {}: {} — db-sync FAILED",
                    t.getClass().getName(), t.getMessage());
            state = STATE_FAILED;
            this.engine = null;
        }
    }

    @Override
    public void stop() {
        CdcEngine running = engine;
        if (running != null) {
            try {
                running.stop();
            } catch (Throwable t) {
                log.error("CdcEngine.stop threw {}", t.getClass().getName());
            }
            engine = null;
        }
    }

    @Override
    public void onDisconnect() {
        source = null;
        provider = null;
        context = null;
        statsTracker = null;
        engine = null;
    }

    @Override
    public ModuleStatus currentStatus() {
        PoolStats poolStats = null;
        JdbcConnectionSource src = source;
        if (src != null) {
            try {
                Optional<PoolStats> pool = src.stats();
                if (pool != null && pool.isPresent()) {
                    poolStats = pool.get();
                }
            } catch (Throwable t) {
                log.warn("JdbcConnectionSource.stats threw {}", t.getClass().getName());
            }
        }

        List<EntityStats> entityStats = null;
        EntityStatsTracker tracker = statsTracker;
        if (tracker != null) {
            try {
                List<EntityStats> entities = tracker.currentStatuses();
                if (entities != null && !entities.isEmpty()) {
                    entityStats = entities;
                }
            } catch (Throwable t) {
                log.warn("EntityStatsTracker.currentStatuses threw {}", t.getClass().getName());
            }
        }

        ModuleStatus.Stats stats;
        if (poolStats == null && entityStats == null) {
            stats = ModuleStatus.Stats.empty();
        } else {
            stats = ModuleStatus.Stats.builder().pool(poolStats).entities(entityStats).build();
        }
        return ModuleStatus.builder()
                .name(NAME)
                .state(state)
                .stats(stats)
                .build();
    }

    private static boolean performSmokeCheck(JdbcConnectionSource src) {
        try (Connection c = src.getConnection()) {
            c.setReadOnly(true);
            if (c.isValid(SMOKE_VALID_TIMEOUT_SECONDS)) {
                return true;
            }
            log.error("Smoke check failed: connection not valid within {}s", SMOKE_VALID_TIMEOUT_SECONDS);
            return false;
        } catch (SQLException | RuntimeException e) {
            log.error("Smoke check threw {}: {}", e.getClass().getName(), e.getMessage());
            return false;
        }
    }

    private static List<JdbcConnectionSource> loadJdbc() {
        return loadAll(JdbcConnectionSource.class);
    }

    private static List<DbSchemaProvider> loadSchema() {
        return loadAll(DbSchemaProvider.class);
    }

    private static <T> List<T> loadAll(Class<T> spi) {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DbSyncModule.class.getClassLoader());
            ServiceLoader<T> loader = ServiceLoader.load(spi);
            List<T> result = new ArrayList<T>();
            for (T item : loader) {
                result.add(item);
            }
            return Collections.unmodifiableList(result);
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    private static void sendViaNxKafka(String topic, byte[] key, Object value,
                                       org.apache.kafka.clients.producer.Callback callback) {
        try {
            NxKafka.instance().send(topic, key, value, callback);
        } catch (KafkaException notConfigured) {
            log.warn("NxKafka not configured — db-sync send dropped (topic={})", topic);
            try {
                callback.onCompletion(null, notConfigured);
            } catch (Throwable t) {
                log.warn("KafkaSender callback threw {}", t.getClass().getName());
            }
        }
    }

    private static String namesOf(List<JdbcConnectionSource> impls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < impls.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(impls.get(i).getClass().getName());
        }
        return sb.toString();
    }

    private static <T> String classNamesOf(List<T> impls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < impls.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(impls.get(i).getClass().getName());
        }
        return sb.toString();
    }
}
