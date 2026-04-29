package app.l2nx.gs.adapter.api.rest;

import java.util.*;

/**
 * Adapter handshake response — identity bundle plus the Kafka context the adapter
 * needs to bootstrap its client, plus per-entity sync topics for DB-reading
 * modules.
 *
 * @see ConnectRequest
 * @see KafkaConfig
 */
public final class ConnectResponse {

    private final UUID tenantId;
    private final String tenantSlug;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final KafkaConfig kafka;
    private final Map<String, String> syncTopics;

    public ConnectResponse(UUID tenantId,
                           String tenantSlug,
                           UUID serverId,
                           String serverSlug,
                           String serverName,
                           KafkaConfig kafka,
                           Map<String, String> syncTopics) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.kafka = kafka;
        this.syncTopics = syncTopics == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(syncTopics));
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

    public KafkaConfig getKafka() {
        return kafka;
    }

    /**
     * Per-entity Kafka topic names delivered by the platform. Keyed by entity name
     * ({@code "clan"}, {@code "character"}, …); value is the fully-qualified topic
     * the adapter is authorized to publish that entity's {@code SyncEvent}s into
     * (e.g. {@code "bohpts.gs.sync.clans"}).
     *
     * <p>{@code null} (field absent on the wire) and an empty map are operationally
     * equivalent — db-sync transitions to {@code DISABLED} on either. Adapter does
     * NOT validate topic names, does NOT pre-flight existence on the Kafka cluster,
     * does NOT create topics.</p>
     *
     * <p>The map is immutable: defensively copied on construction; consumers see
     * an unmodifiable view. {@code null} is preserved on this DTO (not normalized
     * to an empty map) so the wire-shape round-trip stays lossless for the
     * platform-side producer that builds this object before serialization.</p>
     */
    public Map<String, String> getSyncTopics() {
        return syncTopics;
    }

    public Builder toBuilder() {
        return new Builder()
                .tenantId(tenantId)
                .tenantSlug(tenantSlug)
                .serverId(serverId)
                .serverSlug(serverSlug)
                .serverName(serverName)
                .kafka(kafka)
                .syncTopics(syncTopics);
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
                && Objects.equals(syncTopics, that.syncTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName, kafka, syncTopics);
    }

    @Override
    public String toString() {
        return "ConnectResponse[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", kafka=" + kafka
                + ", syncTopics=" + syncTopics + "]";
    }

    public static final class Builder {
        private UUID tenantId;
        private String tenantSlug;
        private UUID serverId;
        private String serverSlug;
        private String serverName;
        private KafkaConfig kafka;
        private Map<String, String> syncTopics;

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

        public Builder kafka(KafkaConfig kafka) {
            this.kafka = kafka;
            return this;
        }

        public Builder syncTopics(Map<String, String> syncTopics) {
            this.syncTopics = syncTopics;
            return this;
        }

        public ConnectResponse build() {
            return new ConnectResponse(tenantId, tenantSlug, serverId, serverSlug,
                    serverName, kafka, syncTopics);
        }
    }
}
