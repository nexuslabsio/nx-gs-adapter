package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.rest.SyncTopics;

import java.util.Objects;
import java.util.UUID;

/**
 * Identity bundle handed to every {@link AdapterModule#onConnect(ConnectContext)} call
 * after a successful platform handshake. Modules cache only the bits they need;
 * the context itself is immutable.
 *
 * <p>Phase 1 carried identity only. Phase 2 added {@link #getSyncTopics()} —
 * namespaced per-entity Kafka topic names delivered by the platform via
 * {@code ConnectResponse.syncTopics}; consumed by sync modules
 * ({@code db-sync}, {@code runtime-sync}). Future phases will extend with
 * operator-config access and a narrow Kafka publish capability.</p>
 */
public final class ConnectContext {

    private final UUID tenantId;
    private final String tenantSlug;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final String adapterVersion;
    private final SyncTopics syncTopics;

    public ConnectContext(UUID tenantId,
                          String tenantSlug,
                          UUID serverId,
                          String serverSlug,
                          String serverName,
                          String adapterVersion,
                          SyncTopics syncTopics) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.syncTopics = syncTopics == null ? new SyncTopics(null, null, null) : syncTopics;
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
     * Namespaced per-entity Kafka topic addressing for sync modules. Always
     * non-null — a {@code null} {@code ConnectResponse.syncTopics} on the wire
     * is normalized here to an empty {@link SyncTopics} (every namespace
     * resolves to an empty map). Modules read their namespace via
     * {@code getSyncTopics().getDb()} / {@code .getRuntime()} / {@code .getDp()}
     * and treat empty as {@code DISABLED}.
     */
    public SyncTopics getSyncTopics() {
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
        private SyncTopics syncTopics;

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

        public Builder syncTopics(SyncTopics syncTopics) {
            this.syncTopics = syncTopics;
            return this;
        }

        public ConnectContext build() {
            return new ConnectContext(tenantId, tenantSlug, serverId, serverSlug,
                    serverName, adapterVersion, syncTopics);
        }
    }
}
