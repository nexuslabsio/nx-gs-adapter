package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;
import java.util.UUID;

/**
 * Adapter handshake response for {@code POST /api/tenants/loginservers/connect}.
 * Mirrors {@link ConnectResponse} for the gameserver host-type, minus the
 * sync-stream topic bundle (login servers never carry DB / runtime / datapack
 * sync data).
 *
 * <p>{@link #getHeartbeatTopic()} is fully-qualified
 * ({@code "<tenant>.ls.<slug>.heartbeat"}). {@link #getMessagingTopics()}
 * {@code events} map carries the {@code account} family
 * ({@code "<tenant>.ls.events.account"}) — single-family today; reserved for
 * future LS-emitted event families without a contract bump.</p>
 *
 * @see ConnectResponse
 * @see KafkaCredentials
 * @see MessagingTopics
 */
public final class LoginServerConnectResponse {

    private final UUID serverId;
    private final String serverSlug;
    private final UUID tenantId;
    private final String tenantSlug;
    private final String serverName;
    private final KafkaCredentials kafka;
    private final String heartbeatTopic;
    private final MessagingTopics messagingTopics;

    public LoginServerConnectResponse(UUID serverId,
                                      String serverSlug,
                                      UUID tenantId,
                                      String tenantSlug,
                                      String serverName,
                                      KafkaCredentials kafka,
                                      String heartbeatTopic,
                                      MessagingTopics messagingTopics) {
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverName = serverName;
        this.kafka = kafka;
        this.heartbeatTopic = heartbeatTopic;
        this.messagingTopics = messagingTopics;
    }

    public UUID getServerId() {
        return serverId;
    }

    public String getServerSlug() {
        return serverSlug;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public String getTenantSlug() {
        return tenantSlug;
    }

    public String getServerName() {
        return serverName;
    }

    public KafkaCredentials getKafka() {
        return kafka;
    }

    public String getHeartbeatTopic() {
        return heartbeatTopic;
    }

    public MessagingTopics getMessagingTopics() {
        return messagingTopics;
    }

    public Builder toBuilder() {
        return new Builder()
                .serverId(serverId)
                .serverSlug(serverSlug)
                .tenantId(tenantId)
                .tenantSlug(tenantSlug)
                .serverName(serverName)
                .kafka(kafka)
                .heartbeatTopic(heartbeatTopic)
                .messagingTopics(messagingTopics);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LoginServerConnectResponse)) return false;
        LoginServerConnectResponse that = (LoginServerConnectResponse) o;
        return Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(tenantId, that.tenantId)
                && Objects.equals(tenantSlug, that.tenantSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(kafka, that.kafka)
                && Objects.equals(heartbeatTopic, that.heartbeatTopic)
                && Objects.equals(messagingTopics, that.messagingTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, serverSlug, tenantId, tenantSlug, serverName,
                kafka, heartbeatTopic, messagingTopics);
    }

    @Override
    public String toString() {
        return "LoginServerConnectResponse[serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverName=" + serverName
                + ", kafka=" + kafka
                + ", heartbeatTopic=" + heartbeatTopic
                + ", messagingTopics=" + messagingTopics + "]";
    }

    public static final class Builder {
        private UUID serverId;
        private String serverSlug;
        private UUID tenantId;
        private String tenantSlug;
        private String serverName;
        private KafkaCredentials kafka;
        private String heartbeatTopic;
        private MessagingTopics messagingTopics;

        public Builder serverId(UUID serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder serverSlug(String serverSlug) {
            this.serverSlug = serverSlug;
            return this;
        }

        public Builder tenantId(UUID tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder tenantSlug(String tenantSlug) {
            this.tenantSlug = tenantSlug;
            return this;
        }

        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public Builder kafka(KafkaCredentials kafka) {
            this.kafka = kafka;
            return this;
        }

        public Builder heartbeatTopic(String heartbeatTopic) {
            this.heartbeatTopic = heartbeatTopic;
            return this;
        }

        public Builder messagingTopics(MessagingTopics messagingTopics) {
            this.messagingTopics = messagingTopics;
            return this;
        }

        public LoginServerConnectResponse build() {
            return new LoginServerConnectResponse(serverId, serverSlug, tenantId, tenantSlug,
                    serverName, kafka, heartbeatTopic, messagingTopics);
        }
    }
}
