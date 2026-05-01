package app.l2nx.gs.commons.concurrent;

import app.l2nx.gs.log.NxLog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SafeRunnableTest {

    @Test
    void wrap_shouldNotPropagate_whenWrappedThrows() {
        RecordingLog log = new RecordingLog();
        Runnable boom = () -> {
            throw new RuntimeException("kaboom");
        };

        Runnable wrapped = SafeRunnable.wrap(boom, log);
        wrapped.run();

        assertEquals(1, log.errorCalls);
        assertTrue(log.lastError.contains("Wrapped runnable threw"),
                "log message should mention wrapped runnable failure");
    }

    @Test
    void wrap_shouldKeepDelegateRunnable_callableAfterFailure() {
        RecordingLog log = new RecordingLog();
        AtomicInteger calls = new AtomicInteger();
        Runnable failOnce = () -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw new RuntimeException("first call fails");
            }
        };

        Runnable wrapped = SafeRunnable.wrap(failOnce, log);
        wrapped.run();
        wrapped.run();
        wrapped.run();

        assertEquals(3, calls.get(), "wrapper must keep dispatching to the delegate");
        assertEquals(1, log.errorCalls, "only the first call's failure was logged");
    }

    @Test
    void wrap_shouldNotPropagate_whenDelegateThrowsError() {
        RecordingLog log = new RecordingLog();
        Runnable err = () -> {
            throw new StackOverflowError("simulated");
        };

        // Wrapper guards Throwable, not just Exception.
        Runnable wrapped = SafeRunnable.wrap(err, log);
        wrapped.run();

        assertEquals(1, log.errorCalls);
    }

    private static final class RecordingLog implements NxLog {
        int errorCalls = 0;
        String lastError = "";
        final List<String> messages = new ArrayList<>();

        @Override
        public void debug(String message, Object... args) {
        }

        @Override
        public void info(String message, Object... args) {
            messages.add(message);
        }

        @Override
        public void warn(String message, Object... args) {
            messages.add(message);
        }

        @Override
        public void error(String message, Object... args) {
            errorCalls++;
            lastError = message;
            messages.add(message);
        }
    }
}
