package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.AdapterConfigFixtures;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

class GameServerConnectFlowTest {

    private static final String CONNECT_PATH = "/api/tenants/gameservers/connect";

    private static final String VALID_GS_RESPONSE = "{"
            + "\"tenantId\":\"00000000-0000-0000-0000-000000000001\","
            + "\"tenantSlug\":\"acme\","
            + "\"serverId\":\"00000000-0000-0000-0000-000000000002\","
            + "\"serverSlug\":\"acme-x1\","
            + "\"serverName\":\"Acme X1\","
            + "\"kafka\":{"
            + "\"bootstrap\":\"kafka.l2nx.app:9092\","
            + "\"securityProtocol\":\"SASL_SSL\","
            + "\"saslMechanism\":\"SCRAM-SHA-256\","
            + "\"saslUsername\":\"acme-x1-user\","
            + "\"saslPassword\":\"redacted\""
            + "},"
            + "\"heartbeatTopic\":\"acme.gs.heartbeat\""
            + "}";

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
    void connect_shouldHitGameServersPath() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_GS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        GameServerConnectFlow flow = new GameServerConnectFlow(cfg);
        TypedConnectOutcome<ConnectResponse> outcome = flow.connect();

        assertEquals(200, outcome.getStatusCode());
        assertTrue(outcome.getResponse().isPresent());
        wireMock.verify(postRequestedFor(urlEqualTo(CONNECT_PATH))
                .withHeader("Authorization",
                        equalTo("Bearer " + AdapterConfigFixtures.VALID_SERVER_KEY)));
    }

    @Test
    void connect_shouldDeserializeConnectResponse() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_GS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        GameServerConnectFlow flow = new GameServerConnectFlow(cfg);
        flow.connect();

        ConnectResponse response = flow.response();
        assertNotNull(response);
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000002"), response.getServerId());
        assertEquals("acme-x1", response.getServerSlug());
        assertEquals("acme", response.getTenantSlug());
    }

    @Test
    void connectPath_shouldReturnGameServerPath() {
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        GameServerConnectFlow flow = new GameServerConnectFlow(cfg);

        assertEquals(CONNECT_PATH, flow.connectPath());
    }

    @Test
    void connect_shouldNotHitLegacyServersPath() {
        // Verify the new adapter version starts hitting /gameservers/connect (not the
        // legacy /servers/connect path that the platform keeps as a dual-mode alias
        // for OLDER adapter deployments during rollout).
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_GS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        new GameServerConnectFlow(cfg).connect();

        wireMock.verify(0, postRequestedFor(urlEqualTo("/api/tenants/servers/connect")));
        wireMock.verify(1, postRequestedFor(urlEqualTo(CONNECT_PATH)));
    }

    @Test
    void accessors_shouldProjectCapturedResponse() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_GS_RESPONSE)));

        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        GameServerConnectFlow flow = new GameServerConnectFlow(cfg);
        flow.connect();

        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000002"), flow.serverId());
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), flow.tenantId());
        assertEquals("acme", flow.tenantSlug());
        assertEquals("acme-x1", flow.serverSlug());
        assertEquals("Acme X1", flow.serverName());
        assertEquals("acme.gs.heartbeat", flow.heartbeatTopic());
        assertNotNull(flow.kafka());
        // GS fixture above carries no syncTopics on the wire → null.
        assertNull(flow.syncTopics());
    }
}
