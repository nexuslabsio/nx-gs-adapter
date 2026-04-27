package app.l2nx.gs.adapter.core;

import app.l2nx.gs.adapter.core.connect.ConnectFlow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NxAdapterStateChangeOrderTest {

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
    void onStateChange_shouldReceiveTransitionsInOrder() {
        List<AdapterState> captured = new ArrayList<>();
        NxAdapter.onStateChange(captured::add);

        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.STARTING);
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.ACTIVE);
        // start() with no l2nx.gs-key fails config resolve → FAILED before CLOSED.
        NxAdapter.start().shutdown();

        assertEquals(Arrays.asList(
                AdapterState.REGISTERING,
                AdapterState.ACTIVE,
                AdapterState.FAILED,
                AdapterState.CLOSED
        ), captured);
    }

    @Test
    void onStateChange_shouldSeeStateAlreadyPublished_whenCallbackInvoked() {
        List<AdapterState> seenInsideCallback = new ArrayList<>();
        NxAdapter.onStateChange(s -> seenInsideCallback.add(NxAdapter.state()));

        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.STARTING);
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.ACTIVE);

        assertEquals(Arrays.asList(AdapterState.REGISTERING, AdapterState.ACTIVE),
                seenInsideCallback);
    }
}
