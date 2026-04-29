package app.l2nx.gs.adapter.api.spi;

import java.util.*;

/**
 * Identity bundle handed to every {@link AdapterModule#onConnect(ConnectContext)} call
 * after a successful platform handshake. Modules cache only the bits they need;
 * the context itself is immutable.
 *
 * <p>Phase 1 carries identity only. Phase 2 adds {@link #getSyncTopics()} —
 * per-entity Kafka topic names delivered by the platform via
 * {@code ConnectResponse.syncTopics}; consumed by sync modules
 * (e.g. {@code db-sync}). Future phases will extend with operator-config access
 * and a narrow Kafka publish capability — kept out of the contract for as long
 * as possible to minimize coupling between {@code nx-gs-adapter-api} and the rest
 * of the stack.</p>
 */
public final class ConnectContext {

    private final UUID tenantId;
    private final String tenantSlug;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final String adapterVersion;
    private final Map<String, String> syncTopics;

    public ConnectContext(UUID tenantId,
                          String tenantSlug,
                          UUID serverId,
                          String serverSlug,
                          String serverName,
                          String adapterVersion,
                          Map<String, String> syncTopics) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.syncTopics = syncTopics == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(syncTopics));
    }

    public UUID getTenantId() {
        return tenantId;
    }

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

    public String getAdapterVersion() {
        return adapterVersion;
    }

    /**
     * Per-entity Kafka topic names delivered by the platform via
     * {@code ConnectResponse.syncTopics}. Keyed by entity name
     * ({@code "clan"}, {@code "character"}, …); value is the fully-qualified topic
     * the adapter is authorized to publish that entity's {@code SyncEvent}s into.
     *
     * <p>Always non-null at this layer — {@code null} from the wire is normalized
     * to an empty map. Modules treat {@code null} and empty wire values
     * identically (both → {@code DISABLED} for sync modules); the wire-level
     * distinction is intentionally erased here so module code only branches on
     * {@code isEmpty()}. The map is unmodifiable.</p>
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
                .adapterVersion(adapterVersion)
                .syncTopics(syncTopics);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectContext)) return false;
        ConnectContext that = (ConnectContext) o;
        return Objects.equals(tenantId, that.tenantId)
                && Objects.equals(tenantSlug, that.tenantSlug)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(adapterVersion, that.adapterVersion)
                && Objects.equals(syncTopics, that.syncTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName,
                adapterVersion, syncTopics);
    }

    @Override
    public String toString() {
        return "ConnectContext[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", adapterVersion=" + adapterVersion
                + ", syncTopics=" + syncTopics + "]";
    }

    public static final class Builder {
        private UUID tenantId;
        private String tenantSlug;
        private UUID serverId;
        private String serverSlug;
        private String serverName;
        private String adapterVersion;
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

        public Builder adapterVersion(String adapterVersion) {
            this.adapterVersion = adapterVersion;
            return this;
        }

        public Builder syncTopics(Map<String, String> syncTopics) {
            this.syncTopics = syncTopics;
            return this;
        }

        public ConnectContext build() {
            return new ConnectContext(tenantId, tenantSlug, serverId, serverSlug,
                    serverName, adapterVersion, syncTopics);
        }
    }
}
