package app.l2nx.gs.adapter.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class NxAdapterDisabledTest {

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
    void start_shouldShortCircuit_whenEnabledFalse() {
        System.setProperty("l2nx.gs-key", "nx_sk_" + repeat('a', 32));
        System.setProperty("l2nx.platform-url", "https://acme.api.l2nx.app");
        System.setProperty("l2nx.enabled", "false");

        NxAdapter.start();

        assertEquals(AdapterState.DISABLED, NxAdapter.state());
        assertFalse(hasAdapterDaemonThread(),
                "No nx-adapter-* threads must be running after a disabled-path start()");
    }

    @Test
    void start_shouldFireSingleDisabledCallback_whenEnabledFalse() {
        System.setProperty("l2nx.gs-key", "nx_sk_" + repeat('b', 32));
        System.setProperty("l2nx.platform-url", "https://acme.api.l2nx.app");
        System.setProperty("l2nx.enabled", "false");

        List<AdapterState> captured = new ArrayList<>();
        NxAdapter.onStateChange(captured::add);

        NxAdapter.start();

        assertEquals(1, captured.size(), "exactly one transition must fire");
        assertEquals(AdapterState.DISABLED, captured.get(0));
    }

    private static boolean hasAdapterDaemonThread() {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("nx-adapter-")) {
                return true;
            }
        }
        return false;
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
