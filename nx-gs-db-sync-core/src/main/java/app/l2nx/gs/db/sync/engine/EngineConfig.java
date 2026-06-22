package app.l2nx.gs.db.sync.engine;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

/**
 * Operator-owned tuning knobs for the CDC engine. Values are read once at engine
 * start and cached for the engine lifetime — no live reload, full handshake
 * required to retune.
 *
 * <p>Source chain (file-first, matching the adapter's bootstrap config):</p>
 * <ol>
 *     <li>{@code l2nx.properties} on disk (path from {@code -Dl2nx.config-file},
 *     or cwd default).</li>
 *     <li>JVM system properties as fallback.</li>
 * </ol>
 *
 * <p>Knob keys are namespaced under {@code l2nx.cdc-engine.*}.</p>
 *
 * <p><strong>Pool sizing vs persistence latency:</strong> snapshot checkpoint
 * (write + fsync) runs inline on the CDC pool worker thread that just finished
 * the entity's cycle. On a 6.5M-entry entity (~78 MB on disk) a fsync can
 * take 50–200 ms on local SSD, 0.2–1 s on HDD / dev-container loopback, or
 * several seconds on network-mounted storage. With the default 300 s throttle
 * a single entity flushes ~12×/hour; on a 2-worker pool with 4 entities that
 * works out to ~3 % worker utilization even on pathologically slow disks,
 * but operators on very slow storage may want to bump {@link #KEY_WORKERS}
 * past the {@code max(2, cores/2)} default to absorb the tail latency.</p>
 */
public final class EngineConfig {

    public static final String KEY_TICK_INTERVAL_SECONDS = "l2nx.cdc-engine.tick-interval-seconds";
    public static final String KEY_ROWS_PER_WINDOW = "l2nx.cdc-engine.rows-per-window";
    public static final String KEY_QUERY_TIMEOUT_SECONDS = "l2nx.cdc-engine.query-timeout-seconds";
    public static final String KEY_PUBLISH_FLUSH_SECONDS = "l2nx.cdc-engine.publish-flush-seconds";
    public static final String KEY_FETCH_SIZE = "l2nx.cdc-engine.fetch-size";
    public static final String KEY_WORKERS = "l2nx.cdc-engine.workers";
    public static final String KEY_PERSIST_DIR = "l2nx.cdc-engine.persist.dir";
    public static final String KEY_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS =
            "l2nx.cdc-engine.persist.checkpoint-min-interval-seconds";

    public static final int DEFAULT_TICK_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_ROWS_PER_WINDOW = 500_000;
    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_PUBLISH_FLUSH_SECONDS = 5;
    public static final int DEFAULT_FETCH_SIZE = 10_000;
    public static final int DEFAULT_WORKERS = 0;
    public static final String DEFAULT_PERSIST_DIR = "nx-cdc-snapshot";
    public static final int DEFAULT_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS = 300;

    static final int MAX_ROWS_PER_WINDOW = 10_000_000;

    private final int tickIntervalSeconds;
    private final int rowsPerWindow;
    private final int queryTimeoutSeconds;
    private final int publishFlushSeconds;
    private final int fetchSize;
    private final int workers;
    private final String persistDir;
    private final int persistCheckpointMinIntervalSeconds;

    public EngineConfig(int tickIntervalSeconds, int rowsPerWindow, int queryTimeoutSeconds, int publishFlushSeconds) {
        this(
                tickIntervalSeconds,
                rowsPerWindow,
                queryTimeoutSeconds,
                publishFlushSeconds,
                DEFAULT_FETCH_SIZE,
                DEFAULT_WORKERS,
                DEFAULT_PERSIST_DIR,
                DEFAULT_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS);
    }

    public EngineConfig(
            int tickIntervalSeconds,
            int rowsPerWindow,
            int queryTimeoutSeconds,
            int publishFlushSeconds,
            int fetchSize,
            int workers) {
        this(
                tickIntervalSeconds,
                rowsPerWindow,
                queryTimeoutSeconds,
                publishFlushSeconds,
                fetchSize,
                workers,
                DEFAULT_PERSIST_DIR,
                DEFAULT_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS);
    }

    public EngineConfig(
            int tickIntervalSeconds,
            int rowsPerWindow,
            int queryTimeoutSeconds,
            int publishFlushSeconds,
            int fetchSize,
            int workers,
            String persistDir,
            int persistCheckpointMinIntervalSeconds) {
        if (rowsPerWindow > MAX_ROWS_PER_WINDOW) {
            throw new IllegalStateException("Invalid '" + KEY_ROWS_PER_WINDOW + "' value " + rowsPerWindow
                    + ": must be <= " + MAX_ROWS_PER_WINDOW
                    + " (sanity cap — larger windows risk OOM on hash buffers)");
        }
        if (persistDir == null || persistDir.trim().isEmpty()) {
            throw new IllegalStateException("Invalid '" + KEY_PERSIST_DIR + "': must be non-empty");
        }
        if (persistCheckpointMinIntervalSeconds < 0) {
            throw new IllegalStateException("Invalid '" + KEY_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS + "' value "
                    + persistCheckpointMinIntervalSeconds + ": must be >= 0");
        }
        this.tickIntervalSeconds = tickIntervalSeconds;
        this.rowsPerWindow = rowsPerWindow;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.publishFlushSeconds = publishFlushSeconds;
        this.fetchSize = fetchSize;
        this.workers = workers;
        this.persistDir = persistDir;
        this.persistCheckpointMinIntervalSeconds = persistCheckpointMinIntervalSeconds;
    }

    public int tickIntervalSeconds() {
        return tickIntervalSeconds;
    }

    public int rowsPerWindow() {
        return rowsPerWindow;
    }

    public int queryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public int publishFlushSeconds() {
        return publishFlushSeconds;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public int workers() {
        return workers;
    }

    public String persistDir() {
        return persistDir;
    }

    public int persistCheckpointMinIntervalSeconds() {
        return persistCheckpointMinIntervalSeconds;
    }

    public static EngineConfig defaults() {
        return new EngineConfig(
                DEFAULT_TICK_INTERVAL_SECONDS,
                DEFAULT_ROWS_PER_WINDOW,
                DEFAULT_QUERY_TIMEOUT_SECONDS,
                DEFAULT_PUBLISH_FLUSH_SECONDS,
                DEFAULT_FETCH_SIZE,
                DEFAULT_WORKERS,
                DEFAULT_PERSIST_DIR,
                DEFAULT_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS);
    }

    public static EngineConfig fromProductionChain() {
        return from(productionChain());
    }

    public static Function<String, String> productionChain() {
        Properties fileProps = loadFileProperties(System::getProperty);
        return fileFirstChain(fileProps, System::getProperty);
    }

    public static EngineConfig fromSystemProperties() {
        return from(System::getProperty);
    }

    public static EngineConfig from(Function<String, String> source) {
        return new EngineConfig(
                positiveInt(source, KEY_TICK_INTERVAL_SECONDS, DEFAULT_TICK_INTERVAL_SECONDS),
                positiveIntCapped(source, KEY_ROWS_PER_WINDOW, DEFAULT_ROWS_PER_WINDOW, MAX_ROWS_PER_WINDOW),
                positiveInt(source, KEY_QUERY_TIMEOUT_SECONDS, DEFAULT_QUERY_TIMEOUT_SECONDS),
                positiveInt(source, KEY_PUBLISH_FLUSH_SECONDS, DEFAULT_PUBLISH_FLUSH_SECONDS),
                positiveInt(source, KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE),
                nonNegativeInt(source, KEY_WORKERS, DEFAULT_WORKERS),
                stringOrDefault(source, KEY_PERSIST_DIR, DEFAULT_PERSIST_DIR),
                nonNegativeInt(
                        source,
                        KEY_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS,
                        DEFAULT_PERSIST_CHECKPOINT_MIN_INTERVAL_SECONDS));
    }

    static Function<String, String> fileFirstChain(Properties fileProps, Function<String, String> sysprops) {
        return key -> {
            String fromFile = fileProps.getProperty(key);
            if (fromFile != null && !fromFile.trim().isEmpty()) {
                return fromFile;
            }
            return sysprops.apply(key);
        };
    }

    static Properties loadFileProperties(Function<String, String> sysprops) {
        return loadFileProperties(sysprops, Paths.get(DEFAULT_FILE_NAME));
    }

    static Properties loadFileProperties(Function<String, String> sysprops, Path defaultPath) {
        String explicit = sysprops.apply(CONFIG_FILE_KEY);
        if (explicit != null && !explicit.trim().isEmpty()) {
            return loadFromPath(Paths.get(explicit.trim()), true);
        }
        return loadFromPath(defaultPath, false);
    }

    private static Properties loadFromPath(Path path, boolean required) {
        Properties props = new Properties();
        try (Reader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            props.load(r);
            return props;
        } catch (NoSuchFileException missing) {
            if (required) {
                throw new IllegalStateException(
                        "cdc-engine config file '" + path + "' (from -D" + CONFIG_FILE_KEY + ") does not exist",
                        missing);
            }
            return props;
        } catch (IOException ioe) {
            throw new IllegalStateException(
                    "Unable to read cdc-engine config file '" + path + "': " + ioe.getMessage(), ioe);
        }
    }

    private static final String DEFAULT_FILE_NAME = "l2nx.properties";
    private static final String CONFIG_FILE_KEY = "l2nx.config-file";

    private static int positiveInt(Function<String, String> source, String key, int defaultValue) {
        String raw = source.apply(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid '" + key + "' value '" + raw + "': not an integer", e);
        }
        if (parsed <= 0) {
            throw new IllegalStateException("Invalid '" + key + "' value " + parsed + ": must be > 0");
        }
        return parsed;
    }

    private static int positiveIntCapped(Function<String, String> source, String key, int defaultValue, int maxValue) {
        int parsed = positiveInt(source, key, defaultValue);
        if (parsed > maxValue) {
            throw new IllegalStateException("Invalid '" + key + "' value " + parsed + ": must be <= " + maxValue
                    + " (sanity cap — larger windows risk OOM on hash buffers)");
        }
        return parsed;
    }

    private static int nonNegativeInt(Function<String, String> source, String key, int defaultValue) {
        String raw = source.apply(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid '" + key + "' value '" + raw + "': not an integer", e);
        }
        if (parsed < 0) {
            throw new IllegalStateException("Invalid '" + key + "' value " + parsed + ": must be >= 0");
        }
        return parsed;
    }

    private static String stringOrDefault(Function<String, String> source, String key, String defaultValue) {
        String raw = source.apply(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        return raw.trim();
    }

    private static boolean booleanFlag(Function<String, String> source, String key, boolean defaultValue) {
        String raw = source.apply(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        String trimmed = raw.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return true;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return false;
        }
        throw new IllegalStateException("Invalid '" + key + "' value '" + raw + "': must be 'true' or 'false'");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EngineConfig)) return false;
        EngineConfig that = (EngineConfig) o;
        return tickIntervalSeconds == that.tickIntervalSeconds
                && rowsPerWindow == that.rowsPerWindow
                && queryTimeoutSeconds == that.queryTimeoutSeconds
                && publishFlushSeconds == that.publishFlushSeconds
                && fetchSize == that.fetchSize
                && workers == that.workers
                && persistCheckpointMinIntervalSeconds == that.persistCheckpointMinIntervalSeconds
                && Objects.equals(persistDir, that.persistDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tickIntervalSeconds,
                rowsPerWindow,
                queryTimeoutSeconds,
                publishFlushSeconds,
                fetchSize,
                workers,
                persistDir,
                persistCheckpointMinIntervalSeconds);
    }

    @Override
    public String toString() {
        return "EngineConfig[tickIntervalSeconds=" + tickIntervalSeconds
                + ", rowsPerWindow=" + rowsPerWindow
                + ", queryTimeoutSeconds=" + queryTimeoutSeconds
                + ", publishFlushSeconds=" + publishFlushSeconds
                + ", fetchSize=" + fetchSize
                + ", workers=" + workers
                + ", persistDir=" + persistDir
                + ", persistCheckpointMinIntervalSeconds=" + persistCheckpointMinIntervalSeconds
                + "]";
    }
}
