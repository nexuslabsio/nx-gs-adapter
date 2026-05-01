package app.l2nx.gs.adapter.core.lifecycle;

import app.l2nx.gs.log.NxLog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StartupBannerTest {

    @Test
    void emit_shouldRenderVersionInOutput() {
        RecordingLog log = new RecordingLog();

        StartupBanner.emit(log, "1.2.3");

        boolean hasWordmark = log.infoMessages.stream().anyMatch(s -> s.contains("L2NX"));
        boolean hasVersion = log.infoMessages.stream().anyMatch(s -> s.contains("1.2.3"));
        assertTrue(hasWordmark);
        assertTrue(hasVersion);
    }

    @Test
    void emit_shouldPadWithBlankLines_topAndBottom() {
        RecordingLog log = new RecordingLog();

        StartupBanner.emit(log, "0.0.0-test");

        assertEquals("", log.infoMessages.get(0));
        assertEquals("", log.infoMessages.get(log.infoMessages.size() - 1));
    }

    private static final class RecordingLog implements NxLog {
        final List<String> infoMessages = new ArrayList<>();

        @Override
        public void debug(String message, Object... args) {
        }

        @Override
        public void info(String message, Object... args) {
            String resolved = message;
            for (Object arg : args) {
                int idx = resolved.indexOf("{}");
                if (idx < 0) break;
                resolved = resolved.substring(0, idx)
                        + arg
                        + resolved.substring(idx + 2);
            }
            infoMessages.add(resolved);
        }

        @Override
        public void warn(String message, Object... args) {
        }

        @Override
        public void error(String message, Object... args) {
        }
    }
}
