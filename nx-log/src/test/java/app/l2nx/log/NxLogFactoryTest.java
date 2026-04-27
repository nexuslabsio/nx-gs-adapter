package app.l2nx.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NxLogFactoryTest {

    @Test
    void getLogger_shouldReturnNonNull() {
        NxLog log = NxLogFactory.getLogger(NxLogFactoryTest.class);
        assertNotNull(log);
    }

    @Test
    void isSlf4jAvailable_shouldReturnTrue_whenBindingPresent() {
        assertTrue(NxLogFactory.isSlf4jAvailable());
    }

    @Test
    void getLogger_shouldReturnSlf4jImpl_whenBindingPresent() {
        NxLog log = NxLogFactory.getLogger(NxLogFactoryTest.class);
        assertEquals("Slf4jNxLog", log.getClass().getSimpleName());
    }

    @Test
    void log_shouldNotThrow_whenCalledAtAnyLevel() {
        NxLog log = NxLogFactory.getLogger(NxLogFactoryTest.class);

        assertDoesNotThrow(() -> log.debug("debug {}", "test"));
        assertDoesNotThrow(() -> log.info("info {}", "test"));
        assertDoesNotThrow(() -> log.warn("warn {}", "test"));
        assertDoesNotThrow(() -> log.error("error {}", "test"));
    }

    @Test
    void log_shouldNotThrow_whenArgsContainNull() {
        NxLog log = NxLogFactory.getLogger(NxLogFactoryTest.class);

        assertDoesNotThrow(() -> log.info("message with null: {}", (Object) null));
        assertDoesNotThrow(() -> log.info("no args message"));
    }

    @Test
    void log_shouldNotThrow_whenLastArgIsThrowable() {
        NxLog log = NxLogFactory.getLogger(NxLogFactoryTest.class);
        Exception ex = new RuntimeException("test error");

        assertDoesNotThrow(() -> log.error("failed: {}", ex.getMessage(), ex));
    }
}
