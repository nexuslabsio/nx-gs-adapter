package app.l2nx.gs.adapter.core.connect.flow;

import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.core.connect.model.TypedConnectOutcome;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Single-attempt handshake strategy; retry belongs to {@link ConnectFlow}. Accessors return {@code null}
 * until a {@link #connect()} succeeds. Internal, not an SPI: public only for {@code NxAdapter}, abstract methods
 * may be added freely.
 */
public interface HostConnectFlow<R> {

    /** Never throws; failures are reported through the outcome. */
    TypedConnectOutcome<R> connect();

    String connectPath();

    @Nullable
    R response();

    @Nullable
    String heartbeatTopic();

    @Nullable
    MessagingTopics topics();

    /** Always {@code null} for login servers, which carry no sync streams. */
    @Nullable
    SyncTopics syncTopics();

    @Nullable
    UUID serverId();

    @Nullable
    UUID tenantId();

    @Nullable
    String tenantSlug();

    @Nullable
    String serverSlug();

    @Nullable
    String serverName();

    @Nullable
    KafkaCredentials kafka();
}
