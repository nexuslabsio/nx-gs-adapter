package app.l2nx.gs.runtime.sync.engine;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EngineConfigTest {

    @Test
    void defaults_shouldBeTenAndFive() {
        EngineConfig cfg = EngineConfig.defaults();

        assertEquals(10, cfg.tickIntervalSeconds());
        assertEquals(5, cfg.publishFlushSeconds());
    }

    @Test
    void from_shouldReadOverrides() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("l2nx.runtime-sync.tick-interval-seconds", "30");
        source.put("l2nx.runtime-sync.publish-flush-seconds", "8");

        EngineConfig cfg = EngineConfig.from(source::get);

        assertEquals(30, cfg.tickIntervalSeconds());
        assertEquals(8, cfg.publishFlushSeconds());
    }

    @Test
    void from_shouldFallBackToDefaults_whenKeyAbsent() {
        EngineConfig cfg = EngineConfig.from(k -> null);

        assertEquals(10, cfg.tickIntervalSeconds());
        assertEquals(5, cfg.publishFlushSeconds());
    }

    @Test
    void from_shouldThrow_whenValueNotInteger() {
        assertThrows(IllegalStateException.class,
                () -> EngineConfig.from(k -> "l2nx.runtime-sync.tick-interval-seconds".equals(k)
                        ? "abc"
                        : null));
    }

    @Test
    void from_shouldThrow_whenValueNotPositive() {
        assertThrows(IllegalStateException.class,
                () -> EngineConfig.from(k -> "l2nx.runtime-sync.tick-interval-seconds".equals(k)
                        ? "0"
                        : null));
    }
}
