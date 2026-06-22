package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.*;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * {@link HostConnectFlow} for gameserver host-type. POSTs to
 * {@code /api/tenants/gameservers/connect} and deserializes the body into
 * {@link ConnectResponse}. New gameserver adapter deployments hit this path;
 * the platform serves {@code /api/tenants/servers/connect} as a dual-mode
 * alias for older adapter versions that pre-date the rename.
 */
public final class GameServerConnectFlow implements HostConnectFlow<ConnectResponse> {

    public static final String CONNECT_PATH = "/api/tenants/gameservers/connect";

    private final AdapterConfig config;
    private final TypedHttpConnect<ConnectResponse> http;
    private volatile @Nullable ConnectResponse captured;

    public GameServerConnectFlow(AdapterConfig config) {
        this(config, new TypedHttpConnect<ConnectResponse>(ConnectResponse.class));
    }

    GameServerConnectFlow(AdapterConfig config, TypedHttpConnect<ConnectResponse> http) {
        this.config = config;
        this.http = http;
    }

    @Override
    public TypedConnectOutcome<ConnectResponse> connect() {
        ConnectRequest body = ConnectRequest.builder()
                .adapterVersion(config.getAdapterVersion())
                .build();
        TypedConnectOutcome<ConnectResponse> outcome =
                http.exchange(ConnectFlow.buildUrl(config.getPlatformUrl(), CONNECT_PATH), config.getServerKey(), body);
        outcome.getResponse().ifPresent(r -> captured = r);
        return outcome;
    }

    @Override
    public String connectPath() {
        return CONNECT_PATH;
    }

    @Override
    public @Nullable ConnectResponse response() {
        return captured;
    }

    @Override
    public @Nullable String heartbeatTopic() {
        ConnectResponse r = captured;
        return r != null ? r.getHeartbeatTopic() : null;
    }

    @Override
    public @Nullable MessagingTopics topics() {
        ConnectResponse r = captured;
        return r != null ? r.getMessagingTopics() : null;
    }

    @Override
    public @Nullable SyncTopics syncTopics() {
        ConnectResponse r = captured;
        return r != null ? r.getSyncTopics() : null;
    }

    @Override
    public @Nullable UUID serverId() {
        ConnectResponse r = captured;
        return r != null ? r.getServerId() : null;
    }

    @Override
    public @Nullable UUID tenantId() {
        ConnectResponse r = captured;
        return r != null ? r.getTenantId() : null;
    }

    @Override
    public @Nullable String tenantSlug() {
        ConnectResponse r = captured;
        return r != null ? r.getTenantSlug() : null;
    }

    @Override
    public @Nullable String serverSlug() {
        ConnectResponse r = captured;
        return r != null ? r.getServerSlug() : null;
    }

    @Override
    public @Nullable String serverName() {
        ConnectResponse r = captured;
        return r != null ? r.getServerName() : null;
    }

    @Override
    public @Nullable KafkaCredentials kafka() {
        ConnectResponse r = captured;
        return r != null ? r.getKafka() : null;
    }
}
