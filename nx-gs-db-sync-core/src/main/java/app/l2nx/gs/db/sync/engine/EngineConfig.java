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
 * <p>Knob keys are namespaced under {@code l2nx.cdc-engine.*} so they coexist
 * cleanly with the bootstrap keys ({@code l2nx.gs-key}, {@code l2nx.platform-url},
 * {@code l2nx.enabled}).</p>
 */
public final class EngineConfig {

    public static final String KEY_TICK_INTERVAL_SECONDS = "l2nx.cdc-engine.tick-interval-seconds";
    public static final String KEY_ROWS_PER_WINDOW = "l2nx.cdc-engine.rows-per-window";
    public static final String KEY_QUERY_TIMEOUT_SECONDS = "l2nx.cdc-engine.query-timeout-seconds";
    public static final String KEY_PUBLISH_FLUSH_SECONDS = "l2nx.cdc-engine.publish-flush-seconds";

    public static final int DEFAULT_TICK_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_ROWS_PER_WINDOW = 500_000;
    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_PUBLISH_FLUSH_SECONDS = 5;

    private final int tickIntervalSeconds;
    private final int rowsPerWindow;
    private final int queryTimeoutSeconds;
    private final int publishFlushSeconds;

    public EngineConfig(int tickIntervalSeconds,
                        int rowsPerWindow,
                        int queryTimeoutSeconds,
                        int publishFlushSeconds) {
        this.tickIntervalSeconds = tickIntervalSeconds;
        this.rowsPerWindow = rowsPerWindow;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.publishFlushSeconds = publishFlushSeconds;
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

    public static EngineConfig defaults() {
        return new EngineConfig(
                DEFAULT_TICK_INTERVAL_SECONDS,
                DEFAULT_ROWS_PER_WINDOW,
                DEFAULT_QUERY_TIMEOUT_SECONDS,
                DEFAULT_PUBLISH_FLUSH_SECONDS);
    }

    /**
     * Production source chain — same shape as {@code ConfigResolver} in
     * {@code :nx-gs-adapter-core}: file ({@code -Dl2nx.config-file} or cwd
     * default {@code l2nx.properties}) wins over JVM system properties.
     * A missing default file is graceful (sysprops still consulted); an
     * explicitly-pointed file that's unreadable throws.
     */
    public static EngineConfig fromProductionChain() {
        return from(productionChain());
    }

    /**
     * Returns the resolved production source (file-first, sysprop fallback)
     * as a SAM, suitable for handing to callers that own their own
     * {@link #from(Function)} invocation timing.
     */
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
                positiveInt(source, KEY_ROWS_PER_WINDOW, DEFAULT_ROWS_PER_WINDOW),
                positiveInt(source, KEY_QUERY_TIMEOUT_SECONDS, DEFAULT_QUERY_TIMEOUT_SECONDS),
                positiveInt(source, KEY_PUBLISH_FLUSH_SECONDS, DEFAULT_PUBLISH_FLUSH_SECONDS));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EngineConfig)) return false;
        EngineConfig that = (EngineConfig) o;
        return tickIntervalSeconds == that.tickIntervalSeconds
                && rowsPerWindow == that.rowsPerWindow
                && queryTimeoutSeconds == that.queryTimeoutSeconds
                && publishFlushSeconds == that.publishFlushSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tickIntervalSeconds, rowsPerWindow, queryTimeoutSeconds, publishFlushSeconds);
    }

    @Override
    public String toString() {
        return "EngineConfig[tickIntervalSeconds=" + tickIntervalSeconds
                + ", rowsPerWindow=" + rowsPerWindow
                + ", queryTimeoutSeconds=" + queryTimeoutSeconds
                + ", publishFlushSeconds=" + publishFlushSeconds + "]";
    }
}
