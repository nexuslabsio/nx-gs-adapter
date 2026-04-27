package app.l2nx.gs.adapter.core.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

/**
 * Resolves adapter configuration from a two-source chain (per key), file-first:
 * <ol>
 *   <li>Properties file — either the absolute path given by
 *       {@code -Dl2nx.config-file=<path>} (operator-preferred), or the classpath resource
 *       {@code l2nx.properties} as a fallback when {@code l2nx.config-file} is unset.</li>
 *   <li>JVM system property (e.g. {@code -Dl2nx.gs-key=...}) — consulted only when the
 *       file does not provide the key.</li>
 * </ol>
 * Pure JDK — no Spring, no SnakeYAML, no third-party config library. Environment-variable
 * resolution is intentionally absent in 0.1.0; file is the preferred medium and is
 * authoritative when present. Both file and classpath sources are read as UTF-8.
 */
public final class ConfigResolver {

    private static final String CLASSPATH_FILE = "l2nx.properties";
    private static final String CONFIG_FILE_KEY = "l2nx.config-file";

    static final String KEY_SERVER_KEY = "l2nx.gs-key";
    static final String KEY_PLATFORM_URL = "l2nx.platform-url";
    static final String KEY_ENABLED = "l2nx.enabled";

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
        return new AdapterConfig(serverKey, platformUrl, adapterVersion, enabled);
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
        return resolveString(KEY_PLATFORM_URL).orElseThrow(() -> missingValueException(KEY_PLATFORM_URL));
    }

    public boolean resolveEnabled() {
        return resolveBoolean(KEY_ENABLED, false);
    }

    public String resolveAdapterVersion() {
        String version = getClass().getPackage().getImplementationVersion();
        return version != null ? version : "0.0.0-unknown";
    }

    private static IllegalStateException missingValueException(String key) {
        return new IllegalStateException(
                "Missing required configuration '" + key + "'. Provide it via one of: "
                        + "(1) properties file with key " + key + "=<value> "
                        + "(file path from -D" + CONFIG_FILE_KEY + "=<path>, "
                        + "or classpath resource '" + CLASSPATH_FILE + "' when -D" + CONFIG_FILE_KEY + " is not set), "
                        + "(2) JVM system property -D" + key + "=<value> as fallback");
    }

    private static boolean isPresent(String value) {
        return value != null && !value.trim().isEmpty();
    }

    static Properties loadFileProperties(Function<String, String> sysprops) {
        String explicitPath = sysprops.apply(CONFIG_FILE_KEY);
        if (isPresent(explicitPath)) {
            return loadFromPath(explicitPath.trim());
        }
        return loadFromClasspath();
    }

    private static Properties loadFromPath(String path) {
        Properties props = new Properties();
        Path filePath = Paths.get(path);
        try (Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            props.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to read config file specified by -D" + CONFIG_FILE_KEY + "='" + path + "': " + e.getMessage(), e);
        }
        return props;
    }

    private static Properties loadFromClasspath() {
        ClassLoader loader = ConfigResolver.class.getClassLoader();
        if (loader == null) {
            loader = ClassLoader.getSystemClassLoader();
        }
        return loadFromClassLoader(loader, CLASSPATH_FILE);
    }

    static Properties loadFromClassLoader(ClassLoader loader, String resource) {
        Properties props = new Properties();
        try {
            Enumeration<URL> urls = loader.getResources(resource);
            if (!urls.hasMoreElements()) {
                return props;
            }
            URL first = urls.nextElement();
            if (urls.hasMoreElements()) {
                StringBuilder all = new StringBuilder("[").append(first);
                while (urls.hasMoreElements()) {
                    all.append(", ").append(urls.nextElement());
                }
                all.append("]");
                throw new IllegalStateException(
                        "Multiple '" + resource + "' resources found on the classpath: " + all
                                + ". Remove the duplicate or set -D" + CONFIG_FILE_KEY
                                + "=<absolute path> to the canonical file.");
            }
            try (Reader reader = new InputStreamReader(first.openStream(), StandardCharsets.UTF_8)) {
                props.load(reader);
            }
        } catch (IOException ignored) {
            // unreadable classpath entries — treat as empty source, file may still come from sysprop
        }
        return props;
    }
}
