package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.*;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * {@link HostConnectFlow} for login-server host-type. POSTs to
 * {@code /api/tenants/loginservers/connect} and deserializes the body into
 * {@link LoginServerConnectResponse}. LS deployments carry no sync-stream
 * topic bundle (the response shape mirrors the gameserver one minus
 * {@code syncTopics}).
 */
public final class LoginServerConnectFlow implements HostConnectFlow<LoginServerConnectResponse> {

    public static final String CONNECT_PATH = "/api/tenants/loginservers/connect";

    private final AdapterConfig config;
    private final TypedHttpConnect<LoginServerConnectResponse> http;
    private volatile @Nullable LoginServerConnectResponse captured;

    public LoginServerConnectFlow(AdapterConfig config) {
        this(config, new TypedHttpConnect<LoginServerConnectResponse>(LoginServerConnectResponse.class));
    }

    LoginServerConnectFlow(AdapterConfig config, TypedHttpConnect<LoginServerConnectResponse> http) {
        this.config = config;
        this.http = http;
    }

    @Override
    public TypedConnectOutcome<LoginServerConnectResponse> connect() {
        ConnectRequest body = ConnectRequest.builder()
                .adapterVersion(config.getAdapterVersion())
                .build();
        TypedConnectOutcome<LoginServerConnectResponse> outcome = http.exchange(
                ConnectFlow.buildUrl(config.getPlatformUrl(), CONNECT_PATH),
                config.getServerKey(),
                body);
        outcome.getResponse().ifPresent(r -> captured = r);
        return outcome;
    }

    @Override
    public String connectPath() {
        return CONNECT_PATH;
    }

    @Override
    public @Nullable LoginServerConnectResponse response() {
        return captured;
    }

    @Override
    public @Nullable String heartbeatTopic() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getHeartbeatTopic() : null;
    }

    @Override
    public @Nullable MessagingTopics topics() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getMessagingTopics() : null;
    }

    @Override
    public @Nullable SyncTopics syncTopics() {
        return null;
    }

    @Override
    public @Nullable UUID serverId() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getServerId() : null;
    }

    @Override
    public @Nullable UUID tenantId() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getTenantId() : null;
    }

    @Override
    public @Nullable String tenantSlug() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getTenantSlug() : null;
    }

    @Override
    public @Nullable String serverSlug() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getServerSlug() : null;
    }

    @Override
    public @Nullable String serverName() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getServerName() : null;
    }

    @Override
    public @Nullable KafkaCredentials kafka() {
        LoginServerConnectResponse r = captured;
        return r != null ? r.getKafka() : null;
    }
}
