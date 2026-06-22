package app.l2nx.gs.gd.sync;

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
 * Operator-owned tuning for the gd-sync module. Read once at module start and
 * cached for the connection lifetime — no live reload.
 *
 * <p>Source chain (file-first, matching the adapter's bootstrap config):</p>
 * <ol>
 *     <li>{@code l2nx.properties} on disk (path from {@code -Dl2nx.config-file},
 *     or cwd default).</li>
 *     <li>JVM system properties as fallback.</li>
 * </ol>
 *
 * <p>The single knob today is the scheduled-resync interval
 * ({@code l2nx.gd-sync.resync-interval-hours}): {@code 0} (default) or absent
 * disables the scheduler; any {@code >0} value enables it, clamped up to a
 * minimum of {@code 1} hour (guard against an over-frequent full-snapshot
 * burst).</p>
 */
public final class GameDataSyncConfig {

    public static final String KEY_RESYNC_INTERVAL_HOURS = "l2nx.gd-sync.resync-interval-hours";

    public static final int DEFAULT_RESYNC_INTERVAL_HOURS = 0;

    static final int MIN_RESYNC_INTERVAL_HOURS = 1;

    private static final String DEFAULT_FILE_NAME = "l2nx.properties";
    private static final String CONFIG_FILE_KEY = "l2nx.config-file";

    private final int resyncIntervalHours;

    public GameDataSyncConfig(int resyncIntervalHours) {
        this.resyncIntervalHours = resyncIntervalHours;
    }

    /**
     * Resolved scheduled-resync interval in hours. {@code 0} = disabled; any
     * configured positive value is clamped up to {@link #MIN_RESYNC_INTERVAL_HOURS}.
     */
    public int resyncIntervalHours() {
        return resyncIntervalHours;
    }

    /**
     * Whether the periodic resync scheduler should run.
     */
    public boolean scheduledResyncEnabled() {
        return resyncIntervalHours > 0;
    }

    public static GameDataSyncConfig defaults() {
        return new GameDataSyncConfig(DEFAULT_RESYNC_INTERVAL_HOURS);
    }

    public static GameDataSyncConfig fromProductionChain() {
        return from(productionChain());
    }

    public static Function<String, String> productionChain() {
        Properties fileProps = loadFileProperties(System::getProperty);
        return fileFirstChain(fileProps, System::getProperty);
    }

    public static GameDataSyncConfig from(Function<String, String> source) {
        int raw = nonNegativeInt(source, KEY_RESYNC_INTERVAL_HOURS, DEFAULT_RESYNC_INTERVAL_HOURS);
        int clamped = raw > 0 ? Math.max(MIN_RESYNC_INTERVAL_HOURS, raw) : 0;
        return new GameDataSyncConfig(clamped);
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
                        "gd-sync config file '" + path + "' (from -D" + CONFIG_FILE_KEY + ") does not exist", missing);
            }
            return props;
        } catch (IOException ioe) {
            throw new IllegalStateException(
                    "Unable to read gd-sync config file '" + path + "': " + ioe.getMessage(), ioe);
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameDataSyncConfig)) return false;
        GameDataSyncConfig that = (GameDataSyncConfig) o;
        return resyncIntervalHours == that.resyncIntervalHours;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resyncIntervalHours);
    }

    @Override
    public String toString() {
        return "GameDataSyncConfig[resyncIntervalHours=" + resyncIntervalHours + "]";
    }
}
