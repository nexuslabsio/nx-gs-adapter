package app.l2nx.gs.runtime.sync.engine;

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
 * Operator-owned tuning knobs for the runtime-sync engine. Values are read once
 * at engine start and cached for the engine lifetime — no live reload, full
 * handshake required to retune.
 *
 * <p>Source chain (file-first, matching the adapter's bootstrap config):</p>
 * <ol>
 *     <li>{@code l2nx.properties} on disk (path from {@code -Dl2nx.config-file},
 *     or cwd default).</li>
 *     <li>JVM system properties as fallback.</li>
 * </ol>
 *
 * <p>Keys are namespaced under {@code l2nx.runtime-sync.*} — independent of
 * {@code l2nx.cdc-engine.*} so the two engines tune independently (different
 * tick cadences, different latency budgets).</p>
 */
public final class EngineConfig {

    public static final String KEY_TICK_INTERVAL_SECONDS = "l2nx.runtime-sync.tick-interval-seconds";
    public static final String KEY_PUBLISH_FLUSH_SECONDS = "l2nx.runtime-sync.publish-flush-seconds";

    public static final int DEFAULT_TICK_INTERVAL_SECONDS = 10;
    public static final int DEFAULT_PUBLISH_FLUSH_SECONDS = 5;

    private final int tickIntervalSeconds;
    private final int publishFlushSeconds;

    public EngineConfig(int tickIntervalSeconds, int publishFlushSeconds) {
        this.tickIntervalSeconds = tickIntervalSeconds;
        this.publishFlushSeconds = publishFlushSeconds;
    }

    public int tickIntervalSeconds() {
        return tickIntervalSeconds;
    }

    public int publishFlushSeconds() {
        return publishFlushSeconds;
    }

    public static EngineConfig defaults() {
        return new EngineConfig(DEFAULT_TICK_INTERVAL_SECONDS, DEFAULT_PUBLISH_FLUSH_SECONDS);
    }

    /**
     * Production source chain — file ({@code -Dl2nx.config-file} or cwd
     * default {@code l2nx.properties}) wins over JVM system properties.
     */
    public static Function<String, String> productionChain() {
        Properties fileProps = loadFileProperties(System::getProperty);
        return fileFirstChain(fileProps, System::getProperty);
    }

    public static EngineConfig from(Function<String, String> source) {
        return new EngineConfig(
                positiveInt(source, KEY_TICK_INTERVAL_SECONDS, DEFAULT_TICK_INTERVAL_SECONDS),
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
                        "runtime-sync config file '" + path + "' (from -D" + CONFIG_FILE_KEY
                                + ") does not exist", missing);
            }
            return props;
        } catch (IOException ioe) {
            throw new IllegalStateException(
                    "Unable to read runtime-sync config file '" + path + "': " + ioe.getMessage(), ioe);
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
                && publishFlushSeconds == that.publishFlushSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tickIntervalSeconds, publishFlushSeconds);
    }

    @Override
    public String toString() {
        return "EngineConfig[tickIntervalSeconds=" + tickIntervalSeconds
                + ", publishFlushSeconds=" + publishFlushSeconds + "]";
    }
}
