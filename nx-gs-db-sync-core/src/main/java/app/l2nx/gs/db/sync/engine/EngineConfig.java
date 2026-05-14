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
 */
public final class EngineConfig {

    public static final String KEY_TICK_INTERVAL_SECONDS = "l2nx.cdc-engine.tick-interval-seconds";
    public static final String KEY_ROWS_PER_WINDOW = "l2nx.cdc-engine.rows-per-window";
    public static final String KEY_QUERY_TIMEOUT_SECONDS = "l2nx.cdc-engine.query-timeout-seconds";
    public static final String KEY_PUBLISH_FLUSH_SECONDS = "l2nx.cdc-engine.publish-flush-seconds";
    public static final String KEY_FETCH_SIZE = "l2nx.cdc-engine.fetch-size";
    public static final String KEY_WORKERS = "l2nx.cdc-engine.workers";

    public static final int DEFAULT_TICK_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_ROWS_PER_WINDOW = 500_000;
    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_PUBLISH_FLUSH_SECONDS = 5;
    public static final int DEFAULT_FETCH_SIZE = 10_000;
    public static final int DEFAULT_WORKERS = 0;

    static final int MAX_ROWS_PER_WINDOW = 10_000_000;

    private final int tickIntervalSeconds;
    private final int rowsPerWindow;
    private final int queryTimeoutSeconds;
    private final int publishFlushSeconds;
    private final int fetchSize;
    private final int workers;

    public EngineConfig(int tickIntervalSeconds,
                        int rowsPerWindow,
                        int queryTimeoutSeconds,
                        int publishFlushSeconds) {
        this(tickIntervalSeconds, rowsPerWindow, queryTimeoutSeconds, publishFlushSeconds,
                DEFAULT_FETCH_SIZE, DEFAULT_WORKERS);
    }

    public EngineConfig(int tickIntervalSeconds,
                        int rowsPerWindow,
                        int queryTimeoutSeconds,
                        int publishFlushSeconds,
                        int fetchSize,
                        int workers) {
        if (rowsPerWindow > MAX_ROWS_PER_WINDOW) {
            throw new IllegalStateException(
                    "Invalid '" + KEY_ROWS_PER_WINDOW + "' value " + rowsPerWindow
                            + ": must be <= " + MAX_ROWS_PER_WINDOW
                            + " (sanity cap — larger windows risk OOM on hash buffers)");
        }
        this.tickIntervalSeconds = tickIntervalSeconds;
        this.rowsPerWindow = rowsPerWindow;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.publishFlushSeconds = publishFlushSeconds;
        this.fetchSize = fetchSize;
        this.workers = workers;
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

    public static EngineConfig defaults() {
        return new EngineConfig(
                DEFAULT_TICK_INTERVAL_SECONDS,
                DEFAULT_ROWS_PER_WINDOW,
                DEFAULT_QUERY_TIMEOUT_SECONDS,
                DEFAULT_PUBLISH_FLUSH_SECONDS,
                DEFAULT_FETCH_SIZE,
                DEFAULT_WORKERS);
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
                nonNegativeInt(source, KEY_WORKERS, DEFAULT_WORKERS));
    }

    static Function<String, String> fileFirstChain(Properties fileProps,
                                                   Function<String, String> sysprops) {
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
                        "cdc-engine config file '" + path + "' (from -D" + CONFIG_FILE_KEY
                                + ") does not exist", missing);
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
            throw new IllegalStateException(
                    "Invalid '" + key + "' value '" + raw + "': not an integer", e);
        }
        if (parsed <= 0) {
            throw new IllegalStateException(
                    "Invalid '" + key + "' value " + parsed + ": must be > 0");
        }
        return parsed;
    }

    private static int positiveIntCapped(Function<String, String> source, String key,
                                         int defaultValue, int maxValue) {
        int parsed = positiveInt(source, key, defaultValue);
        if (parsed > maxValue) {
            throw new IllegalStateException(
                    "Invalid '" + key + "' value " + parsed + ": must be <= " + maxValue
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
            throw new IllegalStateException(
                    "Invalid '" + key + "' value '" + raw + "': not an integer", e);
        }
        if (parsed < 0) {
            throw new IllegalStateException(
                    "Invalid '" + key + "' value " + parsed + ": must be >= 0");
        }
        return parsed;
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
        throw new IllegalStateException(
                "Invalid '" + key + "' value '" + raw + "': must be 'true' or 'false'");
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
                && workers == that.workers;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tickIntervalSeconds, rowsPerWindow, queryTimeoutSeconds,
                publishFlushSeconds, fetchSize, workers);
    }

    @Override
    public String toString() {
        return "EngineConfig[tickIntervalSeconds=" + tickIntervalSeconds
                + ", rowsPerWindow=" + rowsPerWindow
                + ", queryTimeoutSeconds=" + queryTimeoutSeconds
                + ", publishFlushSeconds=" + publishFlushSeconds
                + ", fetchSize=" + fetchSize
                + ", workers=" + workers + "]";
    }
}
