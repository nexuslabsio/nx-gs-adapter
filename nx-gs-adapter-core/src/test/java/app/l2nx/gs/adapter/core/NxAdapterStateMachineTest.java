package app.l2nx.gs.adapter.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.core.connect.ConnectFlow;
import app.l2nx.gs.adapter.core.kafka.CapturingKafkaFactory;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.kafka.KafkaState;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the {@link NxAdapter} state machine via package-private simulate helpers
 * (no real ConnectFlow or NxKafka) to verify the Kafka-state-coupled mapping rules:
 *
 * <ul>
 *   <li>Pre-first-ACTIVE {@code TRANSIENT} stays in {@code REGISTERING}; post-ACTIVE
 *       drives {@code DEGRADED}.</li>
 *   <li>Kafka {@code CONNECTED} → {@code ACTIVE}, {@code DISCONNECTED} → {@code DEGRADED}
 *       only when adapter is in a live state.</li>
 * </ul>
 */
class NxAdapterStateMachineTest {

    private List<AdapterState> captured;

    @BeforeEach
    void setUp() {
        NxAdapter.resetForTesting();
        captured = new ArrayList<>();
        NxAdapter.onStateChange(captured::add);
    }

    @AfterEach
    void tearDown() {
        NxAdapter.resetForTesting();
    }

    @Test
    void simulateKafkaStateChange_shouldEnterActive_whenConnected() {
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CONNECTED);

        assertEquals(AdapterState.ACTIVE, NxAdapter.state());
        assertEquals(Collections.singletonList(AdapterState.ACTIVE), captured);
    }

    @Test
    void simulateKafkaStateChange_shouldNotPropagateDisconnected_whenStateIsInit() {
        // Pre-first-ACTIVE: a Kafka DISCONNECTED before we ever reached ACTIVE/DEGRADED
        // must not transition the adapter (handshake not yet complete).
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.DISCONNECTED);

        assertEquals(AdapterState.INIT, NxAdapter.state());
        assertEquals(Collections.emptyList(), captured);
    }

    @Test
    void simulateKafkaStateChange_shouldFlipActiveAndDegraded_inLiveCycle() {
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CONNECTED);
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.DISCONNECTED);
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CONNECTED);

        assertEquals(AdapterState.ACTIVE, NxAdapter.state());
        assertEquals(Arrays.asList(AdapterState.ACTIVE, AdapterState.DEGRADED, AdapterState.ACTIVE), captured);
    }

    @Test
    void simulateKafkaStateChange_shouldIgnoreClosedFromKafka() {
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CONNECTED);
        captured.clear();

        // Adapter shutdown drives CLOSED itself — a stray Kafka CLOSED event must be ignored.
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CLOSED);

        assertEquals(AdapterState.ACTIVE, NxAdapter.state());
        assertEquals(Collections.emptyList(), captured);
    }

    @Test
    void simulateConnectOutcome_shouldStayInRegistering_whenTransientPreActive() {
        // Bring adapter to REGISTERING via STARTING.
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.STARTING);
        captured.clear();

        // TRANSIENT before first ACTIVE must NOT downgrade to DEGRADED.
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.TRANSIENT);
        // Subsequent retry fires STARTING again — state cycles inside REGISTERING.
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.STARTING);

        assertEquals(AdapterState.REGISTERING, NxAdapter.state());
        // Only the second STARTING fired a transition (re-asserting REGISTERING).
        assertEquals(Collections.singletonList(AdapterState.REGISTERING), captured);
    }

    @Test
    void simulateConnectOutcome_shouldEnterDegraded_whenTransientPostActive() {
        // Reach ACTIVE via Kafka CONNECTED — this latches wasActive=true.
        NxAdapter.simulateKafkaStateChangeForTesting(KafkaState.CONNECTED);
        captured.clear();

        // Now a TRANSIENT (e.g. from a follow-up connect) must flip to DEGRADED.
        NxAdapter.simulateConnectOutcomeForTesting(ConnectFlow.Outcome.TRANSIENT);

        assertEquals(AdapterState.DEGRADED, NxAdapter.state());
        assertEquals(Collections.singletonList(AdapterState.DEGRADED), captured);
    }

    private static ConnectResponse response(String tenantSlug, String serverSlug, KafkaCredentials kafka) {
        return ConnectResponse.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug(tenantSlug)
                .serverId(UUID.randomUUID())
                .serverSlug(serverSlug)
                .serverName("Test")
                .kafka(kafka)
                .heartbeatTopic("heartbeat")
                .build();
    }

    private static KafkaCredentials validKafkaCredentials() {
        return KafkaCredentials.builder()
                .bootstrap("kafka.l2nx.app:9092")
                .securityProtocol("SASL_SSL")
                .saslMechanism("SCRAM-SHA-256")
                .saslUsername("u")
                .saslPassword("p")
                .build();
    }

    @Test
    void initKafka_shouldComposeClientId_fromTenantAndServerSlug() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        KafkaInitializer init = new KafkaInitializer(factory);

        NxAdapter.simulateInitKafkaForTesting(init, response("acme", "acme-x1", validKafkaCredentials()));

        assertEquals("nx-gs-adapter-acme-acme-x1", factory.capturedClientId);
        assertEquals(AdapterState.ACTIVE, NxAdapter.state());
    }

    @Test
    void initKafka_shouldEnterDegraded_whenKafkaPostBuildStateIsDisconnected() {
        CapturingKafkaFactory factory = new CapturingKafkaFactory();
        factory.postBuildState = KafkaState.DISCONNECTED;
        KafkaInitializer init = new KafkaInitializer(factory);

        NxAdapter.simulateInitKafkaForTesting(init, response("acme", "acme-x1", validKafkaCredentials()));

        assertEquals(AdapterState.DEGRADED, NxAdapter.state());
        assertEquals(1, factory.callCount);
    }
}
