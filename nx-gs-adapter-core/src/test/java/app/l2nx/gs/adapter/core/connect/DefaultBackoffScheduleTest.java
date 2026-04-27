package app.l2nx.gs.adapter.core.connect;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultBackoffScheduleTest {

    private final DefaultBackoffSchedule schedule = new DefaultBackoffSchedule();

    @Test
    void next_shouldFollowCanonicalSchedule() {
        assertEquals(Duration.ofSeconds(30), schedule.next(1));
        assertEquals(Duration.ofMinutes(1), schedule.next(2));
        assertEquals(Duration.ofMinutes(2), schedule.next(3));
        assertEquals(Duration.ofMinutes(5), schedule.next(4));
    }

    @Test
    void next_shouldCapAt5Minutes_afterAttempt4() {
        assertEquals(Duration.ofMinutes(5), schedule.next(5));
        assertEquals(Duration.ofMinutes(5), schedule.next(10));
        assertEquals(Duration.ofMinutes(5), schedule.next(100));
    }

    @Test
    void next_shouldRejectAttemptZeroOrNegative() {
        assertThrows(IllegalArgumentException.class, () -> schedule.next(0));
        assertThrows(IllegalArgumentException.class, () -> schedule.next(-1));
    }
}
