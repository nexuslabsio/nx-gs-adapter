package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.core.concurrent.CapturingScheduler;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.gs.adapter.core.config.AdapterConfigFixtures;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

class ConnectFlowTest {

    private static final String CONNECT_PATH = "/api/tenants/gameservers/connect";

    private static final String VALID_CONNECT_RESPONSE = "{"
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
            + "\"heartbeatTopic\":\"tenants.heartbeat\""
            + "}";

    private WireMockServer wireMock;
    private CapturingScheduler scheduler;
    private List<ConnectFlow.Outcome> outcomes;

    @BeforeEach
    void setUp() {
        wireMock = new WireMockServer(wireMockConfig().dynamicPort());
        wireMock.start();
        scheduler = new CapturingScheduler();
        outcomes = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        wireMock.stop();
    }

    private ConnectFlow newFlow() {
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        return new ConnectFlow(
                new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add);
    }

    @Test
    void run_shouldEmitActiveOutcome_whenStatus200() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_CONNECT_RESPONSE)));

        newFlow().run();

        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING, ConnectFlow.Outcome.ACTIVE), outcomes);
        assertTrue(scheduler.captured.isEmpty(), "no retry should be scheduled on 200");
        wireMock.verify(postRequestedFor(urlEqualTo(CONNECT_PATH))
                .withHeader("Authorization",
                        equalTo("Bearer " + AdapterConfigFixtures.VALID_SERVER_KEY))
                .withRequestBody(matchingJsonPath("$.adapterVersion",
                        equalTo(AdapterConfigFixtures.DEFAULT_VERSION))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dispatchScenarios")
    void run_shouldDispatchPerStatusCode(String label, int status, String body,
                                         ConnectFlow.Outcome expected, boolean retryExpected) {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse()
                        .withStatus(status)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));

        newFlow().run();

        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING, expected), outcomes,
                "outcome for " + label);
        assertEquals(retryExpected ? 1 : 0, scheduler.captured.size(),
                "retry-scheduled for " + label);
        if (retryExpected) {
            long delay = scheduler.captured.get(0).delayMillis;
            assertTrue(delay >= 22_500L && delay <= 37_500L,
                    "first-retry delay for " + label + " out of ±25% jitter window: " + delay);
        }
    }

    static Stream<Arguments> dispatchScenarios() {
        return Stream.of(
                // status, body, expected terminal outcome, whether a retry is scheduled
                Arguments.of("401 → FAILED (invalid server-key)", 401,
                        "{\"code\":\"INVALID_SERVER_KEY\",\"message\":\"x\"}",
                        ConnectFlow.Outcome.FAILED, false),
                Arguments.of("403 GAME_SERVER_DEACTIVATED → REJECTED", 403,
                        "{\"code\":\"GAME_SERVER_DEACTIVATED\",\"message\":\"x\"}",
                        ConnectFlow.Outcome.REJECTED, false),
                Arguments.of("403 other code → FAILED", 403,
                        "{\"code\":\"SOMETHING_ELSE\",\"message\":\"x\"}",
                        ConnectFlow.Outcome.FAILED, false),
                Arguments.of("404 → FAILED", 404, "",
                        ConnectFlow.Outcome.FAILED, false),
                Arguments.of("409 KAFKA_CREDENTIALS_MISSING → TRANSIENT (retry)", 409,
                        "{\"code\":\"KAFKA_CREDENTIALS_MISSING\",\"message\":\"x\"}",
                        ConnectFlow.Outcome.TRANSIENT, true),
                Arguments.of("409 other code → FAILED", 409,
                        "{\"code\":\"OTHER_CONFLICT\",\"message\":\"x\"}",
                        ConnectFlow.Outcome.FAILED, false),
                Arguments.of("500 → TRANSIENT (retry)", 500, "",
                        ConnectFlow.Outcome.TRANSIENT, true),
                Arguments.of("503 → TRANSIENT (retry)", 503, "",
                        ConnectFlow.Outcome.TRANSIENT, true),
                Arguments.of("200 with malformed body → TRANSIENT (retry)", 200,
                        "not-a-json-object",
                        ConnectFlow.Outcome.TRANSIENT, true),
                Arguments.of("200 with empty body → TRANSIENT (retry)", 200, "",
                        ConnectFlow.Outcome.TRANSIENT, true)
        );
    }

    @Test
    void run_shouldEmitTransientOutcome_whenIoFailure() {
        // Capture the URL while WireMock is up, then stop it so the request fails
        // with ConnectException — exercises the IO-failure dispatch branch.
        String baseUrl = wireMock.baseUrl();
        wireMock.stop();

        AdapterConfig cfg = AdapterConfigFixtures.enabled(baseUrl);
        ConnectFlow flow = new ConnectFlow(new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add);
        flow.run();

        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING, ConnectFlow.Outcome.TRANSIENT), outcomes);
        assertEquals(1, scheduler.captured.size());
        long delay = scheduler.captured.get(0).delayMillis;
        assertTrue(delay >= 22_500L && delay <= 37_500L,
                "first-retry delay out of ±25% jitter window: " + delay);
    }

    @Test
    void run_shouldRetryWithBackoff_onTransientFailure() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(503)));

        ConnectFlow flow = newFlow();

        // Each run() schedules one retry; the captured scheduler doesn't fire it
        // automatically. Walk attempts 1..5 manually and assert the canonical
        // 30s → 1m → 2m → 5m schedule, capped at 5m.
        flow.run();
        flow.run();
        flow.run();
        flow.run();
        flow.run();

        assertEquals(5, scheduler.captured.size());
        long[] bases = {30_000L, 60_000L, 120_000L, 300_000L, 300_000L};
        for (int i = 0; i < 5; i++) {
            long q = bases[i] / 4;
            long ms = scheduler.captured.get(i).delayMillis;
            assertTrue(ms >= bases[i] - q && ms <= bases[i] + q,
                    "step " + i + " (base " + bases[i] + ") emitted " + ms
                            + ", out of ±25% jitter window");
        }
    }

    @Test
    void buildUrl_shouldStripTrailingSlash() {
        assertEquals("https://acme.api.l2nx.app/api/tenants/gameservers/connect",
                ConnectFlow.buildUrl("https://acme.api.l2nx.app/", "/api/tenants/gameservers/connect"));
        assertEquals("https://acme.api.l2nx.app/api/tenants/gameservers/connect",
                ConnectFlow.buildUrl("https://acme.api.l2nx.app", "/api/tenants/gameservers/connect"));
    }

    @Test
    void sanitize_shouldRedactBearerTokens() {
        assertEquals("error talking to Bearer ***",
                ConnectFlow.sanitize("error talking to Bearer nx_sk_abcdefghijklmnopqrstuvwxyz012345"));
        assertEquals("(no message)", ConnectFlow.sanitize(null));
        assertEquals("plain text unchanged", ConnectFlow.sanitize("plain text unchanged"));
    }

    @Test
    void run_shouldParseSyncTopics_whenResponseCarriesThem() {
        String body = "{"
                + "\"tenantId\":\"00000000-0000-0000-0000-000000000001\","
                + "\"tenantSlug\":\"acme\","
                + "\"serverId\":\"00000000-0000-0000-0000-000000000002\","
                + "\"serverSlug\":\"acme-x1\","
                + "\"serverName\":\"Acme X1\","
                + "\"kafka\":{\"bootstrap\":\"k:9092\"},"
                + "\"heartbeatTopic\":\"hb\","
                + "\"syncTopics\":{"
                + "\"db\":{"
                + "\"clan\":\"bohpts.gs.sync.db.clan\","
                + "\"character\":\"bohpts.gs.sync.db.character\""
                + "},"
                + "\"runtime\":{"
                + "\"character\":\"bohpts.gs.sync.runtime.character\""
                + "}"
                + "}}";
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));

        AtomicReference<HostConnectFlow<?>> captured = new AtomicReference<HostConnectFlow<?>>();
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        ConnectFlow flow = new ConnectFlow(new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add,
                captured::set);
        flow.run();

        assertEquals(Collections.singletonList(ConnectFlow.Outcome.STARTING), outcomes);
        HostConnectFlow<?> active = captured.get();
        assertNotNull(active);
        ConnectResponse response = (ConnectResponse) active.response();
        assertNotNull(response);
        Map<String, String> expectedDb = new HashMap<String, String>();
        expectedDb.put("clan", "bohpts.gs.sync.db.clan");
        expectedDb.put("character", "bohpts.gs.sync.db.character");
        assertEquals(expectedDb, response.getSyncTopics().getDb());
        assertEquals(Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"),
                response.getSyncTopics().getRuntime());
        assertTrue(response.getSyncTopics().getDp().isEmpty());
        assertEquals("hb", response.getHeartbeatTopic());
    }

    @Test
    void run_shouldExposeNullSyncTopics_whenFieldAbsent() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(VALID_CONNECT_RESPONSE)));

        AtomicReference<HostConnectFlow<?>> captured = new AtomicReference<HostConnectFlow<?>>();
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        ConnectFlow flow = new ConnectFlow(new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add,
                captured::set);
        flow.run();

        HostConnectFlow<?> active = captured.get();
        assertNotNull(active);
        ConnectResponse response = (ConnectResponse) active.response();
        assertNotNull(response);
        assertNull(response.getSyncTopics());
        assertEquals("tenants.heartbeat", response.getHeartbeatTopic());
    }

    @Test
    void run_shouldExposeEmptyNamespaces_whenSyncTopicsEmpty() {
        String body = "{"
                + "\"tenantId\":\"00000000-0000-0000-0000-000000000001\","
                + "\"tenantSlug\":\"acme\","
                + "\"serverId\":\"00000000-0000-0000-0000-000000000002\","
                + "\"serverSlug\":\"acme-x1\","
                + "\"serverName\":\"Acme X1\","
                + "\"kafka\":{\"bootstrap\":\"k:9092\"},"
                + "\"heartbeatTopic\":\"hb\","
                + "\"syncTopics\":{}}";
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));

        AtomicReference<HostConnectFlow<?>> captured = new AtomicReference<HostConnectFlow<?>>();
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        ConnectFlow flow = new ConnectFlow(new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add,
                captured::set);
        flow.run();

        HostConnectFlow<?> active = captured.get();
        assertNotNull(active);
        ConnectResponse response = (ConnectResponse) active.response();
        assertNotNull(response);
        assertNotNull(response.getSyncTopics());
        assertTrue(response.getSyncTopics().getDb().isEmpty());
        assertTrue(response.getSyncTopics().getRuntime().isEmpty());
        assertTrue(response.getSyncTopics().getDp().isEmpty());
    }

    @Test
    void run_shouldNotPropagate_whenAttemptThrows() {
        ConnectFlow flow = new ConnectFlow(
                new ThrowingHostConnectFlow(),
                new DefaultBackoffSchedule(),
                scheduler,
                outcomes::add);

        flow.run();

        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING, ConnectFlow.Outcome.TRANSIENT), outcomes);
        assertEquals(1, scheduler.captured.size());
    }

    @Test
    void run_shouldEmitFailedOutcome_whenSchedulerRejectsRetry() {
        wireMock.stubFor(post(urlEqualTo(CONNECT_PATH))
                .willReturn(aResponse().withStatus(503)));

        java.util.concurrent.ScheduledExecutorService rejecting = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        rejecting.shutdownNow();
        AdapterConfig cfg = AdapterConfigFixtures.enabled(wireMock.baseUrl());
        ConnectFlow flow = new ConnectFlow(new GameServerConnectFlow(cfg),
                new DefaultBackoffSchedule(),
                rejecting,
                outcomes::add);

        flow.run();

        // STARTING → TRANSIENT → FAILED — scheduler.schedule throws so the retry
        // path can't recover; flow signals terminal failure.
        assertEquals(Arrays.asList(ConnectFlow.Outcome.STARTING,
                ConnectFlow.Outcome.TRANSIENT,
                ConnectFlow.Outcome.FAILED), outcomes);
    }

    private static final class ThrowingHostConnectFlow implements HostConnectFlow<ConnectResponse> {
        @Override
        public TypedConnectOutcome<ConnectResponse> connect() {
            throw new RuntimeException("simulated flow bug");
        }

        @Override
        public String connectPath() {
            return "/throwing";
        }

        @Override
        public ConnectResponse response() {
            return null;
        }

        @Override
        public String heartbeatTopic() {
            return null;
        }

        @Override
        public MessagingTopics topics() {
            return null;
        }

        @Override
        public SyncTopics syncTopics() {
            return null;
        }

        @Override
        public UUID serverId() {
            return null;
        }

        @Override
        public UUID tenantId() {
            return null;
        }

        @Override
        public String tenantSlug() {
            return null;
        }

        @Override
        public String serverSlug() {
            return null;
        }

        @Override
        public String serverName() {
            return null;
        }

        @Override
        public KafkaCredentials kafka() {
            return null;
        }
    }
}
