package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;
import java.util.UUID;

/**
 * Adapter handshake response — identity bundle plus the Kafka context the adapter
 * needs to bootstrap its client.
 *
 * @see ConnectRequest
 * @see KafkaConfig
 */

public final class ConnectResponse {

    private final UUID tenantId;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final KafkaConfig kafka;

    public ConnectResponse(UUID tenantId,
                           UUID serverId,
                           String serverSlug,
                           String serverName,
                           KafkaConfig kafka) {
        this.tenantId = tenantId;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.kafka = kafka;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public UUID getServerId() {
        return serverId;
    }

    public String getServerSlug() {
        return serverSlug;
    }

    public String getServerName() {
        return serverName;
    }

    public KafkaConfig getKafka() {
        return kafka;
    }

    public Builder toBuilder() {
        return new Builder()
                .tenantId(tenantId)
                .serverId(serverId)
                .serverSlug(serverSlug)
                .serverName(serverName)
                .kafka(kafka);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectResponse)) return false;
        ConnectResponse that = (ConnectResponse) o;
        return Objects.equals(tenantId, that.tenantId)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(kafka, that.kafka);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, serverId, serverSlug, serverName, kafka);
    }

    @Override
    public String toString() {
        return "ConnectResponse[tenantId=" + tenantId
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", kafka=" + kafka + "]";
    }

    public static final class Builder {
        private UUID tenantId;
        private UUID serverId;
        private String serverSlug;
        private String serverName;
        private KafkaConfig kafka;

        public Builder tenantId(UUID tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder serverId(UUID serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder serverSlug(String serverSlug) {
            this.serverSlug = serverSlug;
            return this;
        }

        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public Builder kafka(KafkaConfig kafka) {
            this.kafka = kafka;
            return this;
        }

        public ConnectResponse build() {
            return new ConnectResponse(tenantId, serverId, serverSlug, serverName, kafka);
        }
    }
}
