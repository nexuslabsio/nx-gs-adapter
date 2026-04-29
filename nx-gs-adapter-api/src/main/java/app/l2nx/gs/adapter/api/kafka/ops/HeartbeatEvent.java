package app.l2nx.gs.adapter.api.kafka.ops;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Heartbeat message published every 60s. Kafka message key is {@code serverId}.
 *
 * <p>{@code uptimeMs} is milliseconds since the most recent successful
 * {@code /connect} (session-scoped — resets on reconnect). Wire-shape unit is
 * milliseconds for consistency with {@code SyncEvent.timestampEpochMs} and
 * {@code EntityStats.lastSyncEpochMs}. Identity fields ({@code tenantId},
 * {@code tenantSlug}, {@code serverId}, {@code serverSlug}, {@code serverName})
 * mirror the values delivered by {@code ConnectResponse} so consumers can route /
 * label heartbeats without a separate lookup.</p>
 *
 * <p>{@code enabledModules} carries one {@link ModuleStatus} per discovered Tier-1
 * module — the platform consumes the list to render per-server module health.</p>
 */
public final class HeartbeatEvent {

    private final String tenantId;
    private final String tenantSlug;
    private final String serverId;
    private final String serverSlug;
    private final String serverName;
    private final String adapterVersion;
    private final long uptimeMs;
    private final List<ModuleStatus> enabledModules;

    public HeartbeatEvent(String tenantId,
                          String tenantSlug,
                          String serverId,
                          String serverSlug,
                          String serverName,
                          String adapterVersion,
                          long uptimeMs,
                          List<ModuleStatus> enabledModules) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.uptimeMs = uptimeMs;
        this.enabledModules = enabledModules == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<ModuleStatus>(enabledModules));
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTenantSlug() {
        return tenantSlug;
    }

    public String getServerId() {
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

    public long getUptimeMs() {
        return uptimeMs;
    }

    public List<ModuleStatus> getEnabledModules() {
        return enabledModules;
    }

    public Builder toBuilder() {
        return new Builder()
                .tenantId(tenantId)
                .tenantSlug(tenantSlug)
                .serverId(serverId)
                .serverSlug(serverSlug)
                .serverName(serverName)
                .adapterVersion(adapterVersion)
                .uptimeMs(uptimeMs)
                .enabledModules(enabledModules);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HeartbeatEvent)) return false;
        HeartbeatEvent that = (HeartbeatEvent) o;
        return uptimeMs == that.uptimeMs
                && Objects.equals(tenantId, that.tenantId)
                && Objects.equals(tenantSlug, that.tenantSlug)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(adapterVersion, that.adapterVersion)
                && Objects.equals(enabledModules, that.enabledModules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName,
                adapterVersion, uptimeMs, enabledModules);
    }

    @Override
    public String toString() {
        return "HeartbeatEvent[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", adapterVersion=" + adapterVersion
                + ", uptimeMs=" + uptimeMs
                + ", enabledModules=" + enabledModules + "]";
    }

    public static final class Builder {
        private String tenantId;
        private String tenantSlug;
        private String serverId;
        private String serverSlug;
        private String serverName;
        private String adapterVersion;
        private long uptimeMs;
        private List<ModuleStatus> enabledModules;

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder tenantSlug(String tenantSlug) {
            this.tenantSlug = tenantSlug;
            return this;
        }

        public Builder serverId(String serverId) {
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

        public Builder uptimeMs(long uptimeMs) {
            this.uptimeMs = uptimeMs;
            return this;
        }

        public Builder enabledModules(List<ModuleStatus> enabledModules) {
            this.enabledModules = enabledModules;
            return this;
        }

        public HeartbeatEvent build() {
            return new HeartbeatEvent(tenantId, tenantSlug, serverId, serverSlug, serverName,
                    adapterVersion, uptimeMs, enabledModules);
        }
    }
}
