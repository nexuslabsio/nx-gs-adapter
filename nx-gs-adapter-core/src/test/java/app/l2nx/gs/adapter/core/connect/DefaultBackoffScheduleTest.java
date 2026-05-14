package app.l2nx.gs.adapter.core.connect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultBackoffScheduleTest {

    private final DefaultBackoffSchedule schedule = new DefaultBackoffSchedule();

    @ParameterizedTest(name = "attempt {0} → base {1}ms ±25% jitter")
    @CsvSource({
            "1, 30000",
            "2, 60000",
            "3, 120000",
            "4, 300000",
            "5, 300000",
            "100, 300000"
    })
    void next_shouldFollowCanonicalSchedule_withinJitterWindow(int attempt, long baseMs) {
        long quarter = baseMs / 4;
        long min = baseMs - quarter;
        long max = baseMs + quarter;
        // Sample multiple times to catch jitter at both edges.
        for (int i = 0; i < 50; i++) {
            long ms = schedule.next(attempt).toMillis();
            assertTrue(ms >= min && ms <= max,
                    "attempt " + attempt + " emitted " + ms + "ms, out of [" + min + ", " + max + "]");
        }
    }

    @Test
    void next_shouldEmitVariedDelays_acrossInvocations() {
        // With ±25% jitter on a 30s base, 25 samples must produce >1 distinct value.
        Set<Long> distinct = new HashSet<>();
        for (int i = 0; i < 25; i++) {
            distinct.add(schedule.next(1).toMillis());
        }
        assertTrue(distinct.size() > 1,
                "expected jitter to surface multiple distinct delays, got " + distinct);
    }

    @Test
    void next_shouldRejectAttemptZeroOrNegative() {
        assertThrows(IllegalArgumentException.class, () -> schedule.next(0));
        assertThrows(IllegalArgumentException.class, () -> schedule.next(-1));
    }

    @SuppressWarnings("unused")
    private static Duration unused() {
        return Duration.ZERO;
    }
}
