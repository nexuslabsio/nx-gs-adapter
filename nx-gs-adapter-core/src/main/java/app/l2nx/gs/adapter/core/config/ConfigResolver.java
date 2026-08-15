package app.l2nx.gs.adapter.core.config;

import app.l2nx.gs.adapter.core.commands.CommandsConfig;
import app.l2nx.gs.adapter.core.events.EventsConfig;
import app.l2nx.gs.adapter.core.events.EventsPublisher;
import app.l2nx.gs.adapter.core.lifecycle.AdapterVersion;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Resolves adapter configuration from a two-source chain (per key), file-first:
 * <ol>
 *   <li>Properties file — either the path given by {@code -Dl2nx.config-file=<path>}
 *       (operator-preferred; absolute or relative to the JVM working directory), or
 *       {@code l2nx.properties} in the JVM working directory of the host application
 *       as a fallback when {@code l2nx.config-file} is unset.</li>
 *   <li>JVM system property (e.g. {@code -Dl2nx.gs-key=...}) — consulted only when the
 *       file does not provide the key.</li>
 * </ol>
 * Pure JDK — no Spring, no SnakeYAML, no third-party config library. Environment-variable
 * resolution is intentionally absent; file is the preferred medium and is authoritative
 * when present. The file is read as UTF-8.
 *
 * <p>Missing-file semantics:</p>
 * <ul>
 *   <li>{@code -Dl2nx.config-file} explicitly set but missing / unreadable / malformed
 *       path → fail loud with {@link IllegalStateException}; the operator's intent is
 *       clear.</li>
 *   <li>{@code -Dl2nx.config-file} unset and {@code l2nx.properties} not present in the
 *       JVM working directory → empty {@link Properties} (graceful — sysprop fallback
 *       may still provide the keys).</li>
 * </ul>
 */
public final class ConfigResolver {

    private static final String DEFAULT_FILE_NAME = "l2nx.properties";
    private static final String CONFIG_FILE_KEY = "l2nx.config-file";

    static final String KEY_SERVER_KEY = "l2nx.gs-key";
    static final String KEY_LS_KEY = "l2nx.ls-key";
    static final String KEY_HOST_TYPE = "l2nx.host-type";
    static final String KEY_PLATFORM_URL = "l2nx.platform-url";
    static final String KEY_ENABLED = "l2nx.enabled";
    static final String KEY_IO_WORKERS = "l2nx.io.workers";

    static final String HOST_TYPE_GS = "gs";
    static final String HOST_TYPE_LS = "ls";
    static final String DEFAULT_HOST_TYPE = HOST_TYPE_GS;

    static final String KEY_KAFKA_BATCH_SIZE = "l2nx.kafka.producer.batch.size";
    static final String KEY_KAFKA_LINGER_MS = "l2nx.kafka.producer.linger.ms";
    static final String KEY_KAFKA_COMPRESSION_TYPE = "l2nx.kafka.producer.compression.type";

    static final String KEY_EVENTS_QUEUE_CAPACITY = "l2nx.events.queue-capacity";
    static final String KEY_EVENTS_DROP_POLICY = "l2nx.events.drop-policy";
    static final String KEY_EVENTS_SHUTDOWN_DRAIN_MS = "l2nx.events.shutdown-drain-timeout-ms";

    static final String KEY_COMMANDS_POLL_TIMEOUT_MS = "l2nx.commands.poll-timeout-ms";
    static final String KEY_COMMANDS_SHUTDOWN_TIMEOUT_MS = "l2nx.commands.shutdown-timeout-ms";
    static final String KEY_COMMANDS_HOST_SYNC_TIMEOUT_MS = "l2nx.commands.host-sync-timeout-ms";
    static final String KEY_COMMANDS_KAFKA_PREFIX = "l2nx.commands.kafka.";

    private static final String SERVER_KEY_PREFIX = "nx_sk_";
    private static final int SERVER_KEY_LENGTH = 38;

    private final Function<String, String> sysprops;
    private final Supplier<Set<String>> syspropNames;
    private final Properties fileProps;

    public ConfigResolver() {
        this(
                System::getProperty,
                () -> new LinkedHashSet<String>(System.getProperties().stringPropertyNames()),
                loadFileProperties(System::getProperty));
    }

    ConfigResolver(Function<String, String> sysprops, Properties fileProps) {
        this(sysprops, Collections::emptySet, fileProps);
    }

    ConfigResolver(Function<String, String> sysprops, Supplier<Set<String>> syspropNames, Properties fileProps) {
        this.sysprops = sysprops;
        this.syspropNames = syspropNames;
        this.fileProps = fileProps;
    }

    public AdapterConfig resolve() {
        String hostType = resolveHostType();
        String serverKey = resolveServerKey(hostType);
        String platformUrl = resolvePlatformUrl();
        String adapterVersion = resolveAdapterVersion();
        boolean enabled = resolveEnabled();
        int ioWorkers = resolveIoWorkers();
        Map<String, Object> kafkaProducerOverrides = resolveKafkaProducerOverrides();
        EventsConfig events = resolveEventsConfig();
        CommandsConfig commands = resolveCommandsConfig();
        return new AdapterConfig(
                serverKey,
                platformUrl,
                adapterVersion,
                enabled,
                ioWorkers,
                kafkaProducerOverrides,
                events,
                commands,
                hostType);
    }

    /**
     * Host-type — selects which connect endpoint the adapter targets and
     * which server-key property name is required. Values: {@code gs} (game
     * server) or {@code ls} (login server). Defaults to {@code gs} for
     * back-compat with existing deployments that pre-date the host-type
     * config key.
     */
    public String resolveHostType() {
        Optional<String> raw = resolveString(KEY_HOST_TYPE);
        if (!raw.isPresent()) {
            return DEFAULT_HOST_TYPE;
        }
        String value = raw.get().trim().toLowerCase(Locale.ROOT);
        if (!HOST_TYPE_GS.equals(value) && !HOST_TYPE_LS.equals(value)) {
            throw new IllegalStateException("Invalid value for '" + KEY_HOST_TYPE + "': '" + raw.get() + "' (expected '"
                    + HOST_TYPE_GS + "' or '" + HOST_TYPE_LS + "')");
        }
        return value;
    }

    public int resolveIoWorkers() {
        int value = resolveInt(KEY_IO_WORKERS, AdapterConfig.defaultIoWorkers());
        if (value < 1) {
            throw new IllegalStateException(
                    "Invalid value for '" + KEY_IO_WORKERS + "': " + value + " (expected positive integer)");
        }
        return value;
    }

    public EventsConfig resolveEventsConfig() {
        int queueCapacity = resolveInt(KEY_EVENTS_QUEUE_CAPACITY, EventsConfig.DEFAULT_QUEUE_CAPACITY);
        if (queueCapacity < 1) {
            throw new IllegalStateException("Invalid value for '" + KEY_EVENTS_QUEUE_CAPACITY + "': " + queueCapacity
                    + " (expected positive integer)");
        }
        EventsPublisher.DropPolicy dropPolicy =
                resolveDropPolicy(KEY_EVENTS_DROP_POLICY, EventsConfig.DEFAULT_DROP_POLICY);
        long shutdownDrainMs = resolveLong(KEY_EVENTS_SHUTDOWN_DRAIN_MS, EventsConfig.DEFAULT_SHUTDOWN_DRAIN_MS);
        if (shutdownDrainMs < 0) {
            throw new IllegalStateException("Invalid value for '" + KEY_EVENTS_SHUTDOWN_DRAIN_MS + "': "
                    + shutdownDrainMs + " (expected non-negative)");
        }
        return new EventsConfig(queueCapacity, dropPolicy, shutdownDrainMs);
    }

    public CommandsConfig resolveCommandsConfig() {
        long pollTimeoutMs = resolveLong(KEY_COMMANDS_POLL_TIMEOUT_MS, CommandsConfig.DEFAULT_POLL_TIMEOUT_MS);
        if (pollTimeoutMs < 1) {
            throw new IllegalStateException("Invalid value for '" + KEY_COMMANDS_POLL_TIMEOUT_MS + "': " + pollTimeoutMs
                    + " (expected positive integer)");
        }
        long shutdownTimeoutMs =
                resolveLong(KEY_COMMANDS_SHUTDOWN_TIMEOUT_MS, CommandsConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        if (shutdownTimeoutMs < 0) {
            throw new IllegalStateException("Invalid value for '" + KEY_COMMANDS_SHUTDOWN_TIMEOUT_MS + "': "
                    + shutdownTimeoutMs + " (expected non-negative)");
        }
        long hostSyncTimeoutMs =
                resolveLong(KEY_COMMANDS_HOST_SYNC_TIMEOUT_MS, CommandsConfig.DEFAULT_HOST_SYNC_TIMEOUT_MS);
        if (hostSyncTimeoutMs < 1) {
            throw new IllegalStateException("Invalid value for '" + KEY_COMMANDS_HOST_SYNC_TIMEOUT_MS + "': "
                    + hostSyncTimeoutMs + " (expected positive integer)");
        }
        Map<String, Object> kafkaOverrides = resolveCommandsKafkaOverrides();
        return new CommandsConfig(pollTimeoutMs, shutdownTimeoutMs, hostSyncTimeoutMs, kafkaOverrides);
    }

    private Map<String, Object> resolveCommandsKafkaOverrides() {
        Map<String, Object> overrides = new LinkedHashMap<String, Object>();
        for (String name : fileProps.stringPropertyNames()) {
            if (name.startsWith(KEY_COMMANDS_KAFKA_PREFIX) && name.length() > KEY_COMMANDS_KAFKA_PREFIX.length()) {
                String kafkaKey = name.substring(KEY_COMMANDS_KAFKA_PREFIX.length());
                String value = fileProps.getProperty(name);
                if (value != null && !value.trim().isEmpty()) {
                    overrides.put(kafkaKey, value.trim());
                }
            }
        }
        // File wins where keys collide — only fill in sysprop-only keys here.
        for (String name : enumerateSyspropNames()) {
            if (!name.startsWith(KEY_COMMANDS_KAFKA_PREFIX) || name.length() == KEY_COMMANDS_KAFKA_PREFIX.length()) {
                continue;
            }
            String kafkaKey = name.substring(KEY_COMMANDS_KAFKA_PREFIX.length());
            if (overrides.containsKey(kafkaKey)) {
                continue;
            }
            String value = sysprops.apply(name);
            if (value != null && !value.trim().isEmpty()) {
                overrides.put(kafkaKey, value.trim());
            }
        }
        return overrides.isEmpty() ? Collections.emptyMap() : overrides;
    }

    private Set<String> enumerateSyspropNames() {
        Set<String> names = syspropNames.get();
        return names != null ? names : Collections.emptySet();
    }

    public int resolveInt(String key, int defaultValue) {
        Optional<String> raw = resolveString(key);
        if (!raw.isPresent()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.get());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid integer value for '" + key + "': '" + raw.get() + "'", e);
        }
    }

    private long resolveLong(String key, long defaultValue) {
        Optional<String> raw = resolveString(key);
        if (!raw.isPresent()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.get());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid long value for '" + key + "': '" + raw.get() + "'", e);
        }
    }

    private EventsPublisher.DropPolicy resolveDropPolicy(String key, EventsPublisher.DropPolicy defaultValue) {
        Optional<String> raw = resolveString(key);
        if (!raw.isPresent()) {
            return defaultValue;
        }
        String value = raw.get();
        try {
            return EventsPublisher.DropPolicy.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Invalid drop-policy value for '" + key + "': '" + value
                            + "' (expected 'oldest' or 'newest', case-insensitive)",
                    e);
        }
    }

    Map<String, Object> resolveKafkaProducerOverrides() {
        Map<String, Object> overrides = new LinkedHashMap<>();
        resolveString(KEY_KAFKA_BATCH_SIZE).ifPresent(v -> overrides.put("batch.size", v));
        resolveString(KEY_KAFKA_LINGER_MS).ifPresent(v -> overrides.put("linger.ms", v));
        resolveString(KEY_KAFKA_COMPRESSION_TYPE).ifPresent(v -> overrides.put("compression.type", v));
        return overrides.isEmpty() ? Collections.emptyMap() : overrides;
    }

    public Optional<String> resolveString(String key) {
        String fromFile = fileProps.getProperty(key);
        if (isPresent(fromFile)) {
            return Optional.of(fromFile.trim());
        }
        String fromSysprop = sysprops.apply(key);
        if (isPresent(fromSysprop)) {
            return Optional.of(fromSysprop.trim());
        }
        return Optional.empty();
    }

    public boolean resolveBoolean(String key, boolean defaultValue) {
        Optional<String> raw = resolveString(key);
        if (!raw.isPresent()) {
            return defaultValue;
        }
        String value = raw.get();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalStateException("Invalid boolean value for '" + key + "': '" + value
                + "' (expected 'true' or 'false', case-insensitive)");
    }

    public String resolveServerKey() {
        return resolveServerKey(DEFAULT_HOST_TYPE);
    }

    /**
     * Resolve the server key matching the host-type. Validation:
     * <ul>
     *   <li>{@code host-type=gs} → exactly {@code l2nx.gs-key} must be
     *   present; setting {@code l2nx.ls-key} alongside is a fatal
     *   misconfiguration.</li>
     *   <li>{@code host-type=ls} → exactly {@code l2nx.ls-key} must be
     *   present; setting {@code l2nx.gs-key} alongside is a fatal
     *   misconfiguration.</li>
     * </ul>
     */
    public String resolveServerKey(String hostType) {
        boolean isGs = HOST_TYPE_GS.equals(hostType);
        String expectedKey = isGs ? KEY_SERVER_KEY : KEY_LS_KEY;
        String otherKey = isGs ? KEY_LS_KEY : KEY_SERVER_KEY;
        boolean otherPresent = resolveString(otherKey).isPresent();
        if (otherPresent) {
            throw new IllegalStateException("Conflicting server-key configuration for host-type='" + hostType
                    + "': '" + otherKey + "' must not be set when '"
                    + KEY_HOST_TYPE + "=" + hostType + "'. Provide '" + expectedKey
                    + "' only.");
        }
        String value = resolveString(expectedKey).orElseThrow(() -> missingValueException(expectedKey));
        if (!value.startsWith(SERVER_KEY_PREFIX) || value.length() != SERVER_KEY_LENGTH) {
            throw new IllegalStateException("Invalid server-key format for '" + expectedKey + "': expected prefix '"
                    + SERVER_KEY_PREFIX + "' and total length " + SERVER_KEY_LENGTH
                    + " (got length " + value.length() + ")");
        }
        return value;
    }

    public String resolvePlatformUrl() {
        String raw = resolveString(KEY_PLATFORM_URL).orElseThrow(() -> missingValueException(KEY_PLATFORM_URL));
        URI uri;
        try {
            uri = new URI(raw);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(
                    "Invalid '" + KEY_PLATFORM_URL + "' value '" + raw + "': " + e.getMessage(), e);
        }
        if (!"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalStateException("Invalid '" + KEY_PLATFORM_URL + "' value '" + raw
                    + "': scheme must be https (server-key would travel in plaintext otherwise)");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
            throw new IllegalStateException("Invalid '" + KEY_PLATFORM_URL + "' value '" + raw + "': missing host");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalStateException("Invalid '" + KEY_PLATFORM_URL + "' value '" + raw
                    + "': must not contain a query string or fragment");
        }
        // Normalize: drop trailing slash so callers can append paths without ambiguity.
        return raw.endsWith("/") ? raw.substring(0, raw.length() - 1) : raw;
    }

    public boolean resolveEnabled() {
        return resolveBoolean(KEY_ENABLED, false);
    }

    public String resolveAdapterVersion() {
        return AdapterVersion.resolve();
    }

    private static IllegalStateException missingValueException(String key) {
        return new IllegalStateException("Missing required configuration '" + key + "'. Provide it via one of: "
                + "(1) properties file with key " + key + "=<value> "
                + "(path from -D" + CONFIG_FILE_KEY + "=<path>, "
                + "or '" + DEFAULT_FILE_NAME + "' in the JVM working directory "
                + "when -D" + CONFIG_FILE_KEY + " is not set), "
                + "(2) JVM system property -D" + key + "=<value> as fallback");
    }

    private static boolean isPresent(String value) {
        return value != null && !value.trim().isEmpty();
    }

    static Properties loadFileProperties(Function<String, String> sysprops) {
        return loadFileProperties(sysprops, Paths.get(DEFAULT_FILE_NAME));
    }

    /**
     * Test seam — lets {@code ConfigResolverTest} aim the cwd-default branch at a
     * temp-directory file without mutating the JVM's {@code user.dir}.
     */
    static Properties loadFileProperties(Function<String, String> sysprops, Path defaultPath) {
        String explicitPath = sysprops.apply(CONFIG_FILE_KEY);
        if (isPresent(explicitPath)) {
            String trimmed = explicitPath.trim();
            Path resolved;
            try {
                resolved = Paths.get(trimmed);
            } catch (InvalidPathException e) {
                throw new IllegalStateException(explicitErrorPrefix(trimmed) + ": invalid path: " + e.getMessage(), e);
            }
            return loadFromPath(resolved, /* required = */ true);
        }
        return loadFromPath(defaultPath, /* required = */ false);
    }

    private static Properties loadFromPath(Path filePath, boolean required) {
        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            props.load(reader);
            return props;
        } catch (NoSuchFileException e) {
            if (required) {
                throw new IllegalStateException(errorPrefix(required, filePath) + ": file does not exist", e);
            }
            return props;
        } catch (IOException e) {
            throw new IllegalStateException(errorPrefix(required, filePath) + ": " + e.getMessage(), e);
        }
    }

    private static String errorPrefix(boolean required, Path filePath) {
        return required
                ? explicitErrorPrefix(filePath.toString())
                : "Unable to read default config file '" + filePath + "'";
    }

    private static String explicitErrorPrefix(String path) {
        return "Unable to read config file specified by -D" + CONFIG_FILE_KEY + "='" + path + "'";
    }
}
