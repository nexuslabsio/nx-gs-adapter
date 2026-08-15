package app.l2nx.gs.gd.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EscalationTrackerTest {

    private static final long GRACE_MS = 1000L;

    private final AtomicLong now = new AtomicLong(0L);
    private final EscalationTracker tracker = new EscalationTracker(GRACE_MS, now::get);

    @Nested
    class Observe {

        @Test
        void observe_shouldReturnFirst_onFirstObservation() {
            assertEquals(EscalationTracker.Stage.FIRST, tracker.observe());
        }

        @Test
        void observe_shouldReturnRepeat_whenInsideGraceWindow() {
            tracker.observe();
            now.set(GRACE_MS - 1L);

            assertEquals(EscalationTracker.Stage.REPEAT, tracker.observe());
        }

        @Test
        void observe_shouldKeepReturningRepeat_forMultipleObservationsInsideGraceWindow() {
            tracker.observe();
            now.set(1L);

            assertEquals(EscalationTracker.Stage.REPEAT, tracker.observe());

            now.set(2L);
            assertEquals(EscalationTracker.Stage.REPEAT, tracker.observe());
        }

        @Test
        void observe_shouldReturnEscalated_onFirstObservationAtOrAfterGraceWindow() {
            tracker.observe();
            now.set(GRACE_MS);

            assertEquals(EscalationTracker.Stage.ESCALATED, tracker.observe());
        }

        @Test
        void observe_shouldReturnSilent_forEveryObservationAfterEscalation() {
            tracker.observe();
            now.set(GRACE_MS);
            tracker.observe();

            now.set(GRACE_MS + 500L);
            assertEquals(EscalationTracker.Stage.SILENT, tracker.observe());
            assertEquals(EscalationTracker.Stage.SILENT, tracker.observe());
        }
    }

    @Nested
    class Reset {

        @Test
        void reset_shouldMakeNextObservationFirst_afterEscalation() {
            tracker.observe();
            now.set(GRACE_MS);
            tracker.observe();

            tracker.reset();

            assertEquals(EscalationTracker.Stage.FIRST, tracker.observe());
        }

        @Test
        void reset_shouldMakeNextObservationFirst_beforeEscalation() {
            tracker.observe();

            tracker.reset();

            assertEquals(EscalationTracker.Stage.FIRST, tracker.observe());
        }
    }
}
