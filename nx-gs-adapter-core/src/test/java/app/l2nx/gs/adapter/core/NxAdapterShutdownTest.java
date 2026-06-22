package app.l2nx.gs.adapter.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NxAdapterShutdownTest {

    private static final String[] L2NX_PROPS = {"l2nx.gs-key", "l2nx.platform-url", "l2nx.enabled", "l2nx.config-file"};

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
    void shutdown_shouldBeIdempotent_whenCalledMultipleTimes() {
        NxAdapter adapter = NxAdapter.start();
        AtomicInteger transitions = new AtomicInteger();
        NxAdapter.onStateChange(s -> transitions.incrementAndGet());

        adapter.shutdown();
        int afterFirst = transitions.get();
        adapter.shutdown();
        adapter.shutdown();

        assertEquals(afterFirst, transitions.get(), "duplicate shutdown() must not emit additional transitions");
        assertEquals(AdapterState.CLOSED, NxAdapter.state());
    }

    @Test
    void shutdown_shouldEmitClosedState() {
        NxAdapter adapter = NxAdapter.start();
        List<AdapterState> captured = new ArrayList<>();
        NxAdapter.onStateChange(captured::add);

        adapter.shutdown();

        assertEquals(AdapterState.CLOSED, NxAdapter.state());
        assertTrue(
                captured.contains(AdapterState.CLOSED),
                "shutdown() must surface a CLOSED transition through onStateChange");
    }
}
