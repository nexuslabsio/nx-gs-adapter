package app.l2nx.gs.adapter.core.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ConfigResolverTest {

    private static final String VALID_KEY = "nx_sk_abcdefghijklmnopqrstuvwxyz012345"; // 38 chars
    private static final String VALID_PLATFORM_URL = "https://acme.api.l2nx.app";

    @Test
    void resolveString_shouldPreferFile_whenBothSourcesPresent() {
        Map<String, String> sys = singletonMap("l2nx.gs-key", "from-sysprop");
        Properties file = props("l2nx.gs-key", "from-file");

        ConfigResolver resolver = new ConfigResolver(sys::get, file);

        assertEquals(Optional.of("from-file"), resolver.resolveString("l2nx.gs-key"));
    }

    @Test
    void resolveString_shouldFallbackToSysprop_whenFileMissingKey() {
        Map<String, String> sys = singletonMap("l2nx.gs-key", "from-sysprop");

        ConfigResolver resolver = new ConfigResolver(sys::get, new Properties());

        assertEquals(Optional.of("from-sysprop"), resolver.resolveString("l2nx.gs-key"));
    }

    @Test
    void resolveString_shouldReturnEmpty_whenNoSourceProvidesValue() {
        ConfigResolver resolver = new ConfigResolver(empty(), new Properties());

        assertEquals(Optional.empty(), resolver.resolveString("l2nx.gs-key"));
    }

    @Test
    void resolveString_shouldTreatBlankValueAsAbsent() {
        Properties file = props("l2nx.gs-key", "   ");
        Map<String, String> sys = singletonMap("l2nx.gs-key", "from-sysprop");

        ConfigResolver resolver = new ConfigResolver(sys::get, file);

        // file value is blank → treated as absent → falls through to sysprop
        assertEquals(Optional.of("from-sysprop"), resolver.resolveString("l2nx.gs-key"));
    }

    @Test
    void resolveString_shouldTrimSurroundingWhitespace_whenValuePresent() {
        Properties file = props("l2nx.platform-url", "  https://acme.api.l2nx.app  ");

        ConfigResolver resolver = new ConfigResolver(empty(), file);

        assertEquals(Optional.of("https://acme.api.l2nx.app"), resolver.resolveString("l2nx.platform-url"));
    }

    @Test
    void resolveServerKey_shouldFailListingBothSources_whenMissing() {
        ConfigResolver resolver = new ConfigResolver(empty(), new Properties());

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolveServerKey);
        assertTrue(ex.getMessage().contains("-Dl2nx.gs-key"));
        assertTrue(ex.getMessage().contains("-Dl2nx.config-file"));
        assertTrue(ex.getMessage().contains("l2nx.properties"));
    }

    @Test
    void resolveServerKey_shouldRejectInvalidServerKeyFormat_whenWrongPrefix() {
        ConfigResolver resolver = withSysprop("l2nx.gs-key", "wrong_abcdefghijklmnopqrstuvwxyz012345");

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolveServerKey);
        assertTrue(ex.getMessage().contains("nx_sk_"));
    }

    @Test
    void resolveServerKey_shouldRejectInvalidServerKeyFormat_whenWrongLength() {
        ConfigResolver resolver = withSysprop("l2nx.gs-key", "nx_sk_short");

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolveServerKey);
        assertTrue(ex.getMessage().contains("38"));
    }

    @Test
    void resolveServerKey_shouldFailMissing_whenValueIsBlank() {
        Properties file = props("l2nx.gs-key", "   ");
        ConfigResolver resolver = new ConfigResolver(empty(), file);

        // blank → treated as absent → "missing" error rather than "invalid format"
        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolveServerKey);
        assertTrue(ex.getMessage().contains("Missing"));
    }

    @Test
    void resolveServerKey_shouldReturnValue_whenFormatValid() {
        ConfigResolver resolver = withSysprop("l2nx.gs-key", VALID_KEY);

        assertEquals(VALID_KEY, resolver.resolveServerKey());
    }

    @Test
    void resolvePlatformUrl_shouldFailWhenMissing() {
        ConfigResolver resolver = new ConfigResolver(empty(), new Properties());

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolvePlatformUrl);
        assertTrue(ex.getMessage().contains("l2nx.platform-url"));
        assertTrue(ex.getMessage().contains("l2nx.config-file"));
    }

    @Test
    void resolvePlatformUrl_shouldFailWhenBlank() {
        Properties file = props("l2nx.platform-url", "    ");
        ConfigResolver resolver = new ConfigResolver(empty(), file);

        assertThrows(IllegalStateException.class, resolver::resolvePlatformUrl);
    }

    @Test
    void resolvePlatformUrl_shouldReturnValue_whenSyspropProvides() {
        ConfigResolver resolver = withSysprop("l2nx.platform-url", VALID_PLATFORM_URL);

        assertEquals(VALID_PLATFORM_URL, resolver.resolvePlatformUrl());
    }

    @Test
    void resolvePlatformUrl_shouldStripTrailingSlash() {
        ConfigResolver resolver = withSysprop("l2nx.platform-url", VALID_PLATFORM_URL + "/");

        assertEquals(VALID_PLATFORM_URL, resolver.resolvePlatformUrl());
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(strings = {
            "http://acme.api.l2nx.app",                // wrong scheme — bearer would travel plaintext
            "ftp://acme.api.l2nx.app",                 // non-http(s) scheme
            "https:///path",                           // missing host
            "https://acme.api.l2nx.app?route=evil",    // query string
            "https://acme.api.l2nx.app#frag",          // fragment
            "https://acme api.l2nx.app"                // malformed URI (space in authority)
    })
    void resolvePlatformUrl_shouldRejectInvalidValues(String value) {
        ConfigResolver resolver = withSysprop("l2nx.platform-url", value);

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolvePlatformUrl);
        assertTrue(ex.getMessage().contains("l2nx.platform-url"),
                "rejection message must reference key, got: " + ex.getMessage());
    }

    @Test
    void resolveEnabled_shouldDefaultToFalse_whenMissing() {
        ConfigResolver resolver = new ConfigResolver(empty(), new Properties());

        assertFalse(resolver.resolveEnabled());
    }

    @Test
    void resolveEnabled_shouldParseTrue_whenSyspropProvidesLowerCase() {
        ConfigResolver resolver = withSysprop("l2nx.enabled", "true");

        assertTrue(resolver.resolveEnabled());
    }

    @Test
    void resolveEnabled_shouldParseTrue_whenFileProvidesUpperCase() {
        Properties file = props("l2nx.enabled", "TRUE");

        ConfigResolver resolver = new ConfigResolver(empty(), file);

        assertTrue(resolver.resolveEnabled());
    }

    @Test
    void resolveEnabled_shouldParseTrue_whenFileProvidesMixedCase() {
        Properties file = props("l2nx.enabled", "True");

        ConfigResolver resolver = new ConfigResolver(empty(), file);

        assertTrue(resolver.resolveEnabled());
    }

    @Test
    void resolveEnabled_shouldReturnFalse_whenValueIsFalse() {
        ConfigResolver resolver = withSysprop("l2nx.enabled", "false");

        assertFalse(resolver.resolveEnabled());
    }

    @Test
    void resolveEnabled_shouldThrow_whenValueIsNotTrueOrFalse() {
        ConfigResolver resolver = withSysprop("l2nx.enabled", "yes");

        IllegalStateException ex = assertThrows(IllegalStateException.class, resolver::resolveEnabled);
        assertTrue(ex.getMessage().contains("yes"));
        assertTrue(ex.getMessage().contains("true"));
    }

    @Test
    void resolve_shouldBuildAdapterConfig_whenAllRequiredPresent() {
        Map<String, String> sys = new HashMap<>();
        sys.put("l2nx.gs-key", VALID_KEY);
        sys.put("l2nx.platform-url", VALID_PLATFORM_URL);
        sys.put("l2nx.enabled", "true");

        ConfigResolver resolver = new ConfigResolver(sys::get, new Properties());
        AdapterConfig config = resolver.resolve();

        assertEquals(VALID_KEY, config.getServerKey());
        assertEquals(VALID_PLATFORM_URL, config.getPlatformUrl());
        assertTrue(config.isEnabled());
        // adapterVersion comes from manifest — null in unit test, falls back to "0.0.0-unknown"
        assertEquals("0.0.0-unknown", config.getAdapterVersion());
    }

    @Test
    void loadFileProperties_shouldUseExplicitPath_whenConfigFileSyspropSet(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("adapter.properties");
        Files.write(configFile,
                ("l2nx.gs-key=" + VALID_KEY + "\nl2nx.enabled=true\n").getBytes(StandardCharsets.UTF_8));

        Map<String, String> sys = singletonMap("l2nx.config-file", configFile.toAbsolutePath().toString());

        Properties loaded = ConfigResolver.loadFileProperties(sys::get);

        assertEquals(VALID_KEY, loaded.getProperty("l2nx.gs-key"));
        assertEquals("true", loaded.getProperty("l2nx.enabled"));
    }

    @Test
    void loadFileProperties_shouldReadAsUtf8_whenFileContainsNonAscii(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("adapter.properties");
        Files.write(configFile, "label=кофе ☕\n".getBytes(StandardCharsets.UTF_8));

        Map<String, String> sys = singletonMap("l2nx.config-file", configFile.toAbsolutePath().toString());

        Properties loaded = ConfigResolver.loadFileProperties(sys::get);

        assertEquals("кофе ☕", loaded.getProperty("label"));
    }

    @Test
    void loadFileProperties_shouldThrow_whenExplicitPathDoesNotExist() {
        Map<String, String> sys = singletonMap("l2nx.config-file", "/nonexistent/path/adapter.properties");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> ConfigResolver.loadFileProperties(sys::get));
        assertTrue(ex.getMessage().contains("l2nx.config-file"));
        assertTrue(ex.getMessage().contains("/nonexistent/path/adapter.properties"));
    }

    @Test
    void loadFileProperties_shouldFallbackToClasspath_whenConfigFileSyspropAbsent() {
        Properties loaded = ConfigResolver.loadFileProperties(empty());

        // No -Dl2nx.config-file set; project test resources do not contain l2nx.properties → empty
        assertTrue(loaded.isEmpty());
    }

    @Test
    void loadFromClassLoader_shouldLoad_whenSingleResourceFound(@TempDir Path tempDir) throws IOException {
        Files.write(tempDir.resolve("test-l2nx.properties"), "k=v\n".getBytes(StandardCharsets.UTF_8));
        try (URLClassLoader loader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()}, null)) {
            Properties loaded = ConfigResolver.loadFromClassLoader(loader, "test-l2nx.properties");
            assertEquals("v", loaded.getProperty("k"));
        }
    }

    @Test
    void loadFromClassLoader_shouldFail_whenMultipleResourcesFound(@TempDir Path tempDir) throws IOException {
        Path dir1 = Files.createDirectory(tempDir.resolve("cp1"));
        Path dir2 = Files.createDirectory(tempDir.resolve("cp2"));
        Files.write(dir1.resolve("test-l2nx.properties"), "k=v1\n".getBytes(StandardCharsets.UTF_8));
        Files.write(dir2.resolve("test-l2nx.properties"), "k=v2\n".getBytes(StandardCharsets.UTF_8));

        try (URLClassLoader loader = new URLClassLoader(
                new URL[]{dir1.toUri().toURL(), dir2.toUri().toURL()}, null)) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> ConfigResolver.loadFromClassLoader(loader, "test-l2nx.properties"));
            assertTrue(ex.getMessage().contains("Multiple"));
            assertTrue(ex.getMessage().contains("test-l2nx.properties"));
        }
    }

    @Test
    void loadFromClassLoader_shouldReturnEmpty_whenNoResourceFound(@TempDir Path tempDir) throws IOException {
        try (URLClassLoader loader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()}, null)) {
            Properties loaded = ConfigResolver.loadFromClassLoader(loader, "nonexistent.properties");
            assertTrue(loaded.isEmpty());
        }
    }

    private static ConfigResolver withSysprop(String key, String value) {
        return new ConfigResolver(singletonMap(key, value)::get, new Properties());
    }

    private static Function<String, String> empty() {
        return key -> null;
    }

    private static Map<String, String> singletonMap(String key, String value) {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private static Properties props(String key, String value) {
        Properties p = new Properties();
        p.setProperty(key, value);
        return p;
    }
}
