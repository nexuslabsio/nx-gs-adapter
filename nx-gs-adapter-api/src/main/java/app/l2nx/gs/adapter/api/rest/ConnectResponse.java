package app.l2nx.gs.adapter.api.rest;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Adapter handshake response — identity bundle plus the Kafka context the adapter
 * needs to bootstrap its client, plus a namespaced {@link SyncTopics} bundle, the
 * heartbeat topic, and a {@link MessagingTopics} bundle for the events / commands
 * surface.
 *
 * @see ConnectRequest
 * @see KafkaCredentials
 * @see SyncTopics
 * @see MessagingTopics
 */
public final class ConnectResponse {

    private final UUID tenantId;
    private final String tenantSlug;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final KafkaCredentials kafka;
    private final @Nullable String heartbeatTopic;
    private final @Nullable SyncTopics syncTopics;
    private final @Nullable MessagingTopics messagingTopics;

    public ConnectResponse(UUID tenantId,
                           String tenantSlug,
                           UUID serverId,
                           String serverSlug,
                           String serverName,
                           KafkaCredentials kafka,
                           @Nullable String heartbeatTopic,
                           @Nullable SyncTopics syncTopics,
                           @Nullable MessagingTopics messagingTopics) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.kafka = kafka;
        this.heartbeatTopic = heartbeatTopic;
        this.syncTopics = syncTopics;
        this.messagingTopics = messagingTopics;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    /**
     * Authoritative tenant slug — the kebab-case identifier the platform issues to the
     * tenant. Source of truth for any consumer that needs to compose tenant-scoped names
     * (e.g. Kafka client IDs); do NOT re-derive from {@code platformUrl}.
     */
    public String getTenantSlug() {
        return tenantSlug;
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

    public KafkaCredentials getKafka() {
        return kafka;
    }

    /**
     * Heartbeat Kafka topic — fully-qualified topic the adapter publishes
     * {@code HeartbeatEvent} into (e.g. {@code "<tenant>.gs.heartbeat"}).
     * {@code null} when the platform omits heartbeat (heartbeat module then
     * stays inactive).
     */
    public @Nullable String getHeartbeatTopic() {
        return heartbeatTopic;
    }

    /**
     * Per-namespace per-entity Kafka topic addressing for sync modules.
     * {@code null} (field absent on the wire) means no sync namespaces are
     * configured — every sync module ({@code db-sync}, {@code runtime-sync},
     * {@code dp-sync}) transitions to {@code DISABLED}.
     */
    public @Nullable SyncTopics getSyncTopics() {
        return syncTopics;
    }

    /**
     * Outbound-events / inbound-commands topic addressing. {@code null} (field
     * absent on the wire) means messaging is unconfigured — every
     * {@code NxEvents.publish(...)} call becomes a no-op + DEBUG log, and
     * inbound commands (Phase 2) stay disabled.
     */
    public @Nullable MessagingTopics getMessagingTopics() {
        return messagingTopics;
    }

    public Builder toBuilder() {
        return new Builder()
                .tenantId(tenantId)
                .tenantSlug(tenantSlug)
                .serverId(serverId)
                .serverSlug(serverSlug)
                .serverName(serverName)
                .kafka(kafka)
                .heartbeatTopic(heartbeatTopic)
                .syncTopics(syncTopics)
                .messagingTopics(messagingTopics);
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
                && Objects.equals(tenantSlug, that.tenantSlug)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(kafka, that.kafka)
                && Objects.equals(heartbeatTopic, that.heartbeatTopic)
                && Objects.equals(syncTopics, that.syncTopics)
                && Objects.equals(messagingTopics, that.messagingTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName,
                kafka, heartbeatTopic, syncTopics, messagingTopics);
    }

    @Override
    public String toString() {
        return "ConnectResponse[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", kafka=" + kafka
                + ", heartbeatTopic=" + heartbeatTopic
                + ", syncTopics=" + syncTopics
                + ", messagingTopics=" + messagingTopics + "]";
    }

    public static final class Builder {
        private UUID tenantId;
        private String tenantSlug;
        private UUID serverId;
        private String serverSlug;
        private String serverName;
        private KafkaCredentials kafka;
        private @Nullable String heartbeatTopic;
        private @Nullable SyncTopics syncTopics;
        private @Nullable MessagingTopics messagingTopics;

        public Builder tenantId(UUID tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder tenantSlug(String tenantSlug) {
            this.tenantSlug = tenantSlug;
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

        public Builder kafka(KafkaCredentials kafka) {
            this.kafka = kafka;
            return this;
        }

        public Builder heartbeatTopic(@Nullable String heartbeatTopic) {
            this.heartbeatTopic = heartbeatTopic;
            return this;
        }

        public Builder syncTopics(@Nullable SyncTopics syncTopics) {
            this.syncTopics = syncTopics;
            return this;
        }

        public Builder messagingTopics(@Nullable MessagingTopics messagingTopics) {
            this.messagingTopics = messagingTopics;
            return this;
        }

        public ConnectResponse build() {
            return new ConnectResponse(tenantId, tenantSlug, serverId, serverSlug,
                    serverName, kafka, heartbeatTopic, syncTopics, messagingTopics);
        }
    }
}
