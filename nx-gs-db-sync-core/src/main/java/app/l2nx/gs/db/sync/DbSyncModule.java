package app.l2nx.gs.db.sync;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Tier-1 module that surfaces a {@link JdbcConnectionSource} chosen via Tier-3 SPI
 * resolution and reports per-tick pool stats in the heartbeat.
 *
 * <p>Phase 1 scope (this slice): SPI plumbing smoke test only — discovery + healthcheck
 * + heartbeat enrichment. The CDC engine, {@code DbSchemaProvider} discovery, and Kafka
 * publishing arrive in Phase 2.</p>
 *
 * <p>State semantics:</p>
 * <ul>
 *     <li>{@code FAILED} — 0 or &gt;1 {@link JdbcConnectionSource} on classpath.</li>
 *     <li>{@code DEGRADED} — exactly one source resolved, but the smoke check
 *     ({@code setReadOnly(true)} + {@code isValid(5)}) failed; module keeps the source
 *     reference so its {@code stats()} still surface in heartbeat.</li>
 *     <li>{@code ACTIVE} — source resolved AND smoke check passed.</li>
 * </ul>
 *
 * <p>Discovered automatically via
 * {@code META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule} — the public
 * no-arg constructor is what {@link ServiceLoader} invokes.</p>
 */
public final class DbSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(DbSyncModule.class);

    static final String NAME = "db-sync";
    static final int SMOKE_VALID_TIMEOUT_SECONDS = 5;

    private final Supplier<List<JdbcConnectionSource>> sourceDiscoverer;
    private final Predicate<JdbcConnectionSource> smokeChecker;

    private volatile String state = "INIT";
    private volatile JdbcConnectionSource source;

    public DbSyncModule() {
        this(DbSyncModule::loadViaServiceLoader, DbSyncModule::performSmokeCheck);
    }

    DbSyncModule(Supplier<List<JdbcConnectionSource>> sourceDiscoverer,
                 Predicate<JdbcConnectionSource> smokeChecker) {
        this.sourceDiscoverer = sourceDiscoverer;
        this.smokeChecker = smokeChecker;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void onConnect(ConnectContext ctx) {
        List<JdbcConnectionSource> sources = sourceDiscoverer.get();
        if (sources.isEmpty()) {
            log.error("No JdbcConnectionSource SPI registered — register one via "
                    + "META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource "
                    + "(see jdbc-connection-source feature docs). db-sync FAILED.");
            state = "FAILED";
            return;
        }
        if (sources.size() > 1) {
            StringBuilder names = new StringBuilder();
            for (int i = 0; i < sources.size(); i++) {
                if (i > 0) names.append(", ");
                names.append(sources.get(i).getClass().getName());
            }
            log.error("Multiple JdbcConnectionSource impls on classpath: [{}]. db-sync FAILED.",
                    names.toString());
            state = "FAILED";
            return;
        }
        JdbcConnectionSource resolved = sources.get(0);
        log.info("JdbcConnectionSource resolved: {}", resolved.name());
        source = resolved;
        state = smokeChecker.test(resolved) ? "ACTIVE" : "DEGRADED";
    }

    @Override
    public void start() {
        // Phase 1 has no ticks to schedule — Phase 2 wires the CDC engine here.
    }

    @Override
    public void stop() {
        // Phase 1 has no in-flight work to drain.
    }

    @Override
    public void onDisconnect() {
        source = null;
    }

    @Override
    public ModuleStatus currentStatus() {
        ModuleStatus.Stats stats = ModuleStatus.Stats.empty();
        JdbcConnectionSource src = source;
        if (src != null) {
            try {
                Optional<PoolStats> pool = src.stats();
                if (pool != null && pool.isPresent()) {
                    stats = ModuleStatus.Stats.builder().pool(pool.get()).build();
                }
            } catch (Throwable t) {
                // A buggy SPI impl shouldn't kill heartbeat enrichment.
                log.warn("JdbcConnectionSource.stats threw {}", t.getClass().getName());
            }
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

    private static List<JdbcConnectionSource> loadViaServiceLoader() {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DbSyncModule.class.getClassLoader());
            ServiceLoader<JdbcConnectionSource> loader = ServiceLoader.load(JdbcConnectionSource.class);
            List<JdbcConnectionSource> result = new ArrayList<JdbcConnectionSource>();
            for (JdbcConnectionSource s : loader) {
                result.add(s);
            }
            return result;
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }
}
