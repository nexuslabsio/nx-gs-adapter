package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.LoginServerConnectResponse;
import app.l2nx.gs.adapter.core.concurrent.CapturingScheduler;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.AdapterConfigFixtures;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

class LoginServerConnectFlowTest {

    private static final String CONNECT_PATH = "/api/tenants/loginservers/connect";

    private static final String VALID_LS_RESPONSE = "{"
            + "\"serverId\":\"00000000-0000-0000-0000-000000000010\","
            + "\"serverSlug\":\"acme-ls\","
            + "\"tenantId\":\"00000000-0000-0000-0000-000000000001\","
            + "\"tenantSlug\":\"acme\","
            + "\"serverName\":\"Acme Login\","
            + "\"kafka\":{"
            + "\"bootstrap\":\"kafka.l2nx.app:9092\","
            + "\"securityProtocol\":\"SASL_SSL\","
            + "\"saslMechanism\":\"SCRAM-SHA-256\","
            + "\"saslUsername\":\"acme-ls-user\","
            + "\"saslPassword\":\"redacted\""
            + "},"
            + "\"heartbeatTopic\":\"acme.ls.acme-ls.heartbeat\","
            + "\"messagingTopics\":{"
            + "\"events\":{\"account\":\"acme.ls.events.account\"}"
            + "}}";

    private WireMockServer wireMock;

    @BeforeEach
    void setUp() {
        wireMock = new WireMockServer(wireMockConfig().dynamicPort());
        wireMock.start();
    }

    @AfterEach
    void tearDown() {
        wireMock.stop();
    }

    @Test
    void connect_shouldHitLoginServersPath() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_LS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        TypedConnectOutcome<LoginServerConnectResponse> outcome = flow.connect();

        assertEquals(200, outcome.getStatusCode());
        assertTrue(outcome.getResponse().isPresent());
        wireMock.verify(postRequestedFor(urlEqualTo(CONNECT_PATH))
                .withHeader("Authorization",
                        equalTo("Bearer " + AdapterConfigFixtures.VALID_SERVER_KEY)));
    }

    @Test
    void connect_shouldDeserializeLoginServerConnectResponse() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_LS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        flow.connect();

        LoginServerConnectResponse response = flow.response();
        assertNotNull(response);
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000010"), response.getServerId());
        assertEquals("acme-ls", response.getServerSlug());
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), response.getTenantId());
        assertEquals("acme", response.getTenantSlug());
        assertEquals("Acme Login", response.getServerName());
        assertEquals("acme.ls.acme-ls.heartbeat", response.getHeartbeatTopic());
        assertNotNull(response.getKafka());
        assertEquals("kafka.l2nx.app:9092", response.getKafka().getBootstrap());
        assertNotNull(response.getMessagingTopics());
        assertEquals("acme.ls.events.account",
                response.getMessagingTopics().getEvents().get("account"));
    }

    @Test
    void accessors_shouldProjectCapturedResponse() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_LS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        flow.connect();

        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000010"), flow.serverId());
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), flow.tenantId());
        assertEquals("acme", flow.tenantSlug());
        assertEquals("acme-ls", flow.serverSlug());
        assertEquals("Acme Login", flow.serverName());
        assertEquals("acme.ls.acme-ls.heartbeat", flow.heartbeatTopic());
        assertNotNull(flow.kafka());
        assertNotNull(flow.topics());
    }

    @Test
    void accessors_shouldReturnNull_beforeFirstConnect() {
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);

        assertNull(flow.response());
        assertNull(flow.serverId());
        assertNull(flow.tenantId());
        assertNull(flow.tenantSlug());
        assertNull(flow.serverSlug());
        assertNull(flow.serverName());
        assertNull(flow.heartbeatTopic());
        assertNull(flow.kafka());
        assertNull(flow.topics());
        assertNull(flow.syncTopics());
    }

    @Test
    void syncTopics_shouldAlwaysReturnNull_forLoginServerHost() {
        // LS deployments carry no sync streams — syncTopics() returns null both
        // before and after a successful connect.
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_LS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        flow.connect();

        assertNull(flow.syncTopics());
    }

    @Test
    void connectPath_shouldReturnLoginServerPath() {
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);

        assertEquals(CONNECT_PATH, flow.connectPath());
    }

    @Test
    void connect_shouldEncodeHttpError_whenNon200() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(401)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"code\":\"INVALID_SERVER_KEY\",\"message\":\"x\"}")));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        TypedConnectOutcome<LoginServerConnectResponse> outcome = flow.connect();

        assertEquals(401, outcome.getStatusCode());
        assertFalse(outcome.getResponse().isPresent());
        assertTrue(outcome.getError().isPresent());
        assertEquals("INVALID_SERVER_KEY", outcome.getError().get().getCode());
        assertNull(flow.response());
    }

    @Test
    void connect_shouldEncodeIoFailure_whenServerUnreachable() {
        String baseUrl = wireMock.baseUrl();
        wireMock.stop();

        AdapterConfig cfg = AdapterConfigFixtures.enabled(baseUrl);
        LoginServerConnectFlow flow = new LoginServerConnectFlow(cfg);
        TypedConnectOutcome<LoginServerConnectResponse> outcome = flow.connect();

        assertTrue(outcome.isIoFailure());
        assertNull(flow.response());
    }

    @Test
    void productionRetryLoop_shouldDriveLoginServerFlow_andDeserializeTypedResponse() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_LS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        LoginServerConnectFlow lsFlow = new LoginServerConnectFlow(cfg);
        CapturingScheduler scheduler = new CapturingScheduler();
        List<ConnectFlow.Outcome> outcomes = new ArrayList<ConnectFlow.Outcome>();
        AtomicReference<HostConnectFlow<?>> active = new AtomicReference<HostConnectFlow<?>>();

        ConnectFlow loop = new ConnectFlow(
                lsFlow,
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add,
                active::set);
        loop.run();

        // STARTING only — onActiveFlow consumed the success path before bare ACTIVE.
        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING), outcomes);
        assertTrue(scheduler.captured.isEmpty(), "no retry on 200");
        HostConnectFlow<?> captured = active.get();
        assertSame(lsFlow, captured, "production loop must pass through the host flow strategy itself");
        Object response = captured.response();
        assertNotNull(response);
        assertEquals(LoginServerConnectResponse.class, response.getClass(),
                "LS retry loop must deserialize as LoginServerConnectResponse, never ConnectResponse");
        assertNotEquals(ConnectResponse.class, response.getClass());
        wireMock.verify(postRequestedFor(urlEqualTo(CONNECT_PATH)));
        wireMock.verify(0, postRequestedFor(urlEqualTo("/api/tenants/gameservers/connect")));
    }
}
