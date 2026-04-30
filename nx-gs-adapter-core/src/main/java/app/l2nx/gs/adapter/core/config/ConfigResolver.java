package app.l2nx.gs.adapter.core.config;

import app.l2nx.gs.adapter.core.lifecycle.AdapterVersion;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.Function;

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
    static final String KEY_PLATFORM_URL = "l2nx.platform-url";
    static final String KEY_ENABLED = "l2nx.enabled";

    static final String KEY_KAFKA_BATCH_SIZE = "l2nx.kafka.producer.batch.size";
    static final String KEY_KAFKA_LINGER_MS = "l2nx.kafka.producer.linger.ms";
    static final String KEY_KAFKA_COMPRESSION_TYPE = "l2nx.kafka.producer.compression.type";

    private static final String SERVER_KEY_PREFIX = "nx_sk_";
    private static final int SERVER_KEY_LENGTH = 38;

    private final Function<String, String> sysprops;
    private final Properties fileProps;

    public ConfigResolver() {
        this(System::getProperty, loadFileProperties(System::getProperty));
    }

    ConfigResolver(Function<String, String> sysprops, Properties fileProps) {
        this.sysprops = sysprops;
        this.fileProps = fileProps;
    }

    public AdapterConfig resolve() {
        String serverKey = resolveServerKey();
        String platformUrl = resolvePlatformUrl();
        String adapterVersion = resolveAdapterVersion();
        boolean enabled = resolveEnabled();
        Map<String, Object> kafkaProducerOverrides = resolveKafkaProducerOverrides();
        return new AdapterConfig(serverKey, platformUrl, adapterVersion, enabled, kafkaProducerOverrides);
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
        throw new IllegalStateException(
                "Invalid boolean value for '" + key + "': '" + value
                        + "' (expected 'true' or 'false', case-insensitive)");
    }

    public String resolveServerKey() {
        String value = resolveString(KEY_SERVER_KEY).orElseThrow(() -> missingValueException(KEY_SERVER_KEY));
        if (!value.startsWith(SERVER_KEY_PREFIX) || value.length() != SERVER_KEY_LENGTH) {
            throw new IllegalStateException(
                    "Invalid server-key format: expected prefix '" + SERVER_KEY_PREFIX
                            + "' and total length " + SERVER_KEY_LENGTH
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
            throw new IllegalStateException(
                    "Invalid '" + KEY_PLATFORM_URL + "' value '" + raw
                            + "': scheme must be https (server-key would travel in plaintext otherwise)");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
            throw new IllegalStateException(
                    "Invalid '" + KEY_PLATFORM_URL + "' value '" + raw + "': missing host");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalStateException(
                    "Invalid '" + KEY_PLATFORM_URL + "' value '" + raw
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
        return new IllegalStateException(
                "Missing required configuration '" + key + "'. Provide it via one of: "
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
                throw new IllegalStateException(
                        explicitErrorPrefix(trimmed) + ": invalid path: " + e.getMessage(), e);
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
                throw new IllegalStateException(
                        errorPrefix(required, filePath) + ": file does not exist", e);
            }
            return props;
        } catch (IOException e) {
            throw new IllegalStateException(
                    errorPrefix(required, filePath) + ": " + e.getMessage(), e);
        }
    }

    private static String errorPrefix(boolean required, Path filePath) {
        return required ? explicitErrorPrefix(filePath.toString())
                : "Unable to read default config file '" + filePath + "'";
    }

    private static String explicitErrorPrefix(String path) {
        return "Unable to read config file specified by -D" + CONFIG_FILE_KEY + "='" + path + "'";
    }
}
