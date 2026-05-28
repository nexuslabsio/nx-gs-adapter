package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Host-type-aware handshake strategy. Performs one POST against the
 * platform's host-type connect endpoint, captures the deserialized response,
 * and exposes uniform accessors so adapter wiring (Kafka client bootstrap,
 * heartbeat, module {@code ConnectContext}) is host-type-agnostic.
 *
 * <p>Two implementations ship with adapter-core:</p>
 * <ul>
 *   <li>{@link GameServerConnectFlow} —
 *   {@code POST /api/tenants/gameservers/connect}, response type
 *   {@link app.l2nx.gs.adapter.api.rest.ConnectResponse}.</li>
 *   <li>{@link LoginServerConnectFlow} —
 *   {@code POST /api/tenants/loginservers/connect}, response type
 *   {@link app.l2nx.gs.adapter.api.rest.LoginServerConnectResponse}.</li>
 * </ul>
 *
 * <p>Single-attempt strategy — retry / backoff / scheduler concerns belong
 * to {@link ConnectFlow}. The field accessors return {@code null} before a
 * successful {@link #connect()}; after success they project the captured
 * response.</p>
 *
 * <p><b>Internal to adapter-core — not an SPI.</b> Public visibility is
 * required only because {@code NxAdapter} (sibling package) consumes the
 * type. External code MUST NOT implement this interface; new abstract
 * methods will be added without deprecation cycles when new host-types or
 * accessor fields land.</p>
 *
 * @param <R> wire response type returned by the host-type endpoint
 */
public interface HostConnectFlow<R> {

    /**
     * Execute one handshake attempt against the platform. The returned
     * {@code TypedConnectOutcome} wraps either the parsed response or a
     * transport / HTTP error — never throws into the caller.
     */
    TypedConnectOutcome<R> connect();

    /**
     * The path under {@code platformUrl} this flow POSTs to — exposed for
     * test introspection and logging.
     */
    String connectPath();

    /**
     * Captured response from the last successful {@link #connect()}.
     * Returns {@code null} before the first successful call.
     */
    @Nullable R response();

    /**
     * Heartbeat Kafka topic from the captured response.
     */
    @Nullable String heartbeatTopic();

    /**
     * Messaging topic bundle (events + commands) from the captured
     * response.
     */
    @Nullable MessagingTopics topics();

    /**
     * Sync-stream topic bundle (db / runtime / datapack). Always {@code null}
     * for login-server host-type — LS deployments carry no sync streams.
     */
    @Nullable SyncTopics syncTopics();

    @Nullable UUID serverId();

    @Nullable UUID tenantId();

    @Nullable String tenantSlug();

    @Nullable String serverSlug();

    @Nullable String serverName();

    /**
     * Kafka client bootstrap credentials from the captured response. Named
     * {@code kafka()} rather than {@code kafkaCredentials()} to match the
     * field accessor naming on the underlying response DTOs.
     */
    @Nullable KafkaCredentials kafka();
}
