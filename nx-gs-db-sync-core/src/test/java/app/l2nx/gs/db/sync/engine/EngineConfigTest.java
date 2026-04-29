package app.l2nx.gs.db.sync.engine;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EngineConfigTest {

    @Test
    void defaults_shouldMatchPublishedDefaults() {
        EngineConfig cfg = EngineConfig.defaults();

        assertEquals(60, cfg.tickIntervalSeconds());
        assertEquals(500_000, cfg.rowsPerWindow());
        assertEquals(10, cfg.queryTimeoutSeconds());
        assertEquals(5, cfg.publishFlushSeconds());
    }

    @Test
    void from_shouldFallBackToDefaults_whenSourceReturnsNullForEveryKey() {
        EngineConfig cfg = EngineConfig.from(k -> null);

        assertEquals(EngineConfig.defaults(), cfg);
    }

    @Test
    void from_shouldOverrideDefaults_whenSourceProvidesValues() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(EngineConfig.KEY_TICK_INTERVAL_SECONDS, "30");
        source.put(EngineConfig.KEY_ROWS_PER_WINDOW, "1000000");
        source.put(EngineConfig.KEY_QUERY_TIMEOUT_SECONDS, "20");
        source.put(EngineConfig.KEY_PUBLISH_FLUSH_SECONDS, "15");

        EngineConfig cfg = EngineConfig.from(source::get);

        assertEquals(30, cfg.tickIntervalSeconds());
        assertEquals(1_000_000, cfg.rowsPerWindow());
        assertEquals(20, cfg.queryTimeoutSeconds());
        assertEquals(15, cfg.publishFlushSeconds());
    }

    @Test
    void from_shouldTrimWhitespace_aroundIntegerValues() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(EngineConfig.KEY_TICK_INTERVAL_SECONDS, "  45  ");

        EngineConfig cfg = EngineConfig.from(source::get);

        assertEquals(45, cfg.tickIntervalSeconds());
    }

    @Test
    void from_shouldThrow_whenValueNotInteger() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(EngineConfig.KEY_TICK_INTERVAL_SECONDS, "abc");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> EngineConfig.from(source::get));
        assertTrue(ex.getMessage().contains(EngineConfig.KEY_TICK_INTERVAL_SECONDS));
    }

    @Test
    void from_shouldThrow_whenValueZeroOrNegative() {
        Map<String, String> source = new HashMap<String, String>();
        source.put(EngineConfig.KEY_ROWS_PER_WINDOW, "0");

        assertThrows(IllegalStateException.class, () -> EngineConfig.from(source::get));

        source.put(EngineConfig.KEY_ROWS_PER_WINDOW, "-5");
        assertThrows(IllegalStateException.class, () -> EngineConfig.from(source::get));
    }
}
