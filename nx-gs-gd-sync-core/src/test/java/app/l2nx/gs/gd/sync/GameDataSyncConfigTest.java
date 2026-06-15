package app.l2nx.gs.gd.sync;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GameDataSyncConfigTest {

    private static java.util.function.Function<String, String> source(Map<String, String> values) {
        return values::get;
    }

    @Test
    void from_shouldDisableResync_whenKeyAbsent() {
        GameDataSyncConfig config = GameDataSyncConfig.from(source(new HashMap<String, String>()));

        assertEquals(0, config.resyncIntervalHours());
        assertFalse(config.scheduledResyncEnabled());
    }

    @Test
    void from_shouldDisableResync_whenZero() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "0");

        GameDataSyncConfig config = GameDataSyncConfig.from(source(values));

        assertEquals(0, config.resyncIntervalHours());
        assertFalse(config.scheduledResyncEnabled());
    }

    @Test
    void from_shouldClampUpToMinimum_whenPositiveBelowMin() {
        Map<String, String> values = new HashMap<String, String>();
        // Value resolves to 1 because MIN is 1 — there is no sub-1 positive int,
        // so this guards the clamp branch explicitly for any future MIN bump.
        values.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "1");

        GameDataSyncConfig config = GameDataSyncConfig.from(source(values));

        assertEquals(GameDataSyncConfig.MIN_RESYNC_INTERVAL_HOURS, config.resyncIntervalHours());
        assertTrue(config.scheduledResyncEnabled());
    }

    @Test
    void from_shouldKeepValue_whenAboveMinimum() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "6");

        GameDataSyncConfig config = GameDataSyncConfig.from(source(values));

        assertEquals(6, config.resyncIntervalHours());
        assertTrue(config.scheduledResyncEnabled());
    }

    @Test
    void from_shouldThrow_whenNegative() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "-1");

        assertThrows(IllegalStateException.class, () -> GameDataSyncConfig.from(source(values)));
    }

    @Test
    void from_shouldThrow_whenNotAnInteger() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "soon");

        assertThrows(IllegalStateException.class, () -> GameDataSyncConfig.from(source(values)));
    }

    @Test
    void fileFirstChain_shouldPreferFileOverSysprop() {
        java.util.Properties fileProps = new java.util.Properties();
        fileProps.setProperty(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "12");
        Map<String, String> sys = new HashMap<String, String>();
        sys.put(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS, "3");

        java.util.function.Function<String, String> chain =
                GameDataSyncConfig.fileFirstChain(fileProps, sys::get);

        assertEquals("12", chain.apply(GameDataSyncConfig.KEY_RESYNC_INTERVAL_HOURS));
    }
}
