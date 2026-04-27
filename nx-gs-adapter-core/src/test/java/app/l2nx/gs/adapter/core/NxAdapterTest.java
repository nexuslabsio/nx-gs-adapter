package app.l2nx.gs.adapter.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NxAdapterTest {

    private static final String[] L2NX_PROPS = {
            "l2nx.gs-key",
            "l2nx.platform-url",
            "l2nx.enabled",
            "l2nx.config-file"
    };

    private final Map<String, String> savedProps = new HashMap<>();

    @BeforeEach
    void clearSysprops() {
        for (String key : L2NX_PROPS) {
            String val = System.getProperty(key);
            if (val != null) {
                savedProps.put(key, val);
                System.clearProperty(key);
            }
        }
        NxAdapter.resetForTesting();
    }

    @AfterEach
    void restoreSysprops() {
        NxAdapter.resetForTesting();
        for (Map.Entry<String, String> e : savedProps.entrySet()) {
            System.setProperty(e.getKey(), e.getValue());
        }
        savedProps.clear();
    }

    @Test
    void start_shouldEnterFailedState_whenConfigResolutionFails() {
        List<AdapterState> captured = new ArrayList<>();
        NxAdapter.onStateChange(captured::add);

        // No l2nx.gs-key in sysprops, no l2nx.properties on test classpath →
        // ConfigResolver throws IllegalStateException → start() must catch and
        // transition to FAILED.
        NxAdapter result = assertDoesNotThrow(NxAdapter::start);

        assertNotNull(result);
        assertEquals(AdapterState.FAILED, NxAdapter.state());
        assertEquals(1, captured.size());
        assertEquals(AdapterState.FAILED, captured.get(0));
    }

    @Test
    void start_shouldNotPropagateException_whenServerKeyFormatInvalid() {
        System.setProperty("l2nx.gs-key", "wrong-prefix");
        System.setProperty("l2nx.platform-url", "https://acme.api.l2nx.app");

        NxAdapter result = assertDoesNotThrow(NxAdapter::start);

        assertNotNull(result);
        assertEquals(AdapterState.FAILED, NxAdapter.state());
    }

    @Test
    void state_shouldDefaultToInit_beforeStart() {
        assertEquals(AdapterState.INIT, NxAdapter.state());
    }

    @Test
    void onStateChange_shouldNotPropagateException_whenCallbackThrows() {
        NxAdapter.onStateChange(s -> {
            throw new RuntimeException("boom");
        });

        // start() with no config → FAILED transition fires the throwing callback;
        // start() must still complete without rethrowing.
        assertDoesNotThrow(NxAdapter::start);
        assertEquals(AdapterState.FAILED, NxAdapter.state());
    }

    @Test
    void start_shouldBeIdempotent_whenCalledMultipleTimes() {
        java.util.concurrent.atomic.AtomicInteger transitions = new java.util.concurrent.atomic.AtomicInteger();
        NxAdapter.onStateChange(s -> transitions.incrementAndGet());

        NxAdapter.start(); // first call: config error → 1 transition (FAILED)
        AdapterState afterFirst = NxAdapter.state();
        int afterFirstCount = transitions.get();

        NxAdapter.start(); // second call: must be a no-op
        NxAdapter.start(); // third call too

        assertEquals(afterFirst, NxAdapter.state(), "state must not change on duplicate start()");
        assertEquals(afterFirstCount, transitions.get(),
                "duplicate start() must not emit additional transitions");
    }
}
