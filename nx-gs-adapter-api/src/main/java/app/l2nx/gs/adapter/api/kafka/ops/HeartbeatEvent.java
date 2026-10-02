package app.l2nx.gs.adapter.api.kafka.ops;

import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Heartbeat published every 60s, keyed by {@code serverId}.
 * {@code uptime} is measured from the latest successful {@code /connect} and serialized as ISO-8601 (e.g. {@code "PT60S"}).
 */
public final class HeartbeatEvent {

    private final String tenantId;
    private final String tenantSlug;
    private final String serverId;
    private final String serverSlug;
    private final String serverName;
    private final String adapterVersion;
    private final Duration uptime;
    private final List<ModuleStatus> enabledModules;

    public HeartbeatEvent(
            String tenantId,
            String tenantSlug,
            String serverId,
            String serverSlug,
            String serverName,
            String adapterVersion,
            Duration uptime,
            List<ModuleStatus> enabledModules) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.uptime = uptime;
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

    public Duration getUptime() {
        return uptime;
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
                .uptime(uptime)
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
        return Objects.equals(uptime, that.uptime)
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
        return Objects.hash(
                tenantId, tenantSlug, serverId, serverSlug, serverName, adapterVersion, uptime, enabledModules);
    }

    @Override
    public String toString() {
        return "HeartbeatEvent[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", adapterVersion=" + adapterVersion
                + ", uptime=" + uptime
                + ", enabledModules=" + enabledModules + "]";
    }

    public static final class Builder {
        private String tenantId;
        private String tenantSlug;
        private String serverId;
        private String serverSlug;
        private String serverName;
        private String adapterVersion;
        private Duration uptime;
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

        public Builder uptime(Duration uptime) {
            this.uptime = uptime;
            return this;
        }

        public Builder enabledModules(List<ModuleStatus> enabledModules) {
            this.enabledModules = enabledModules;
            return this;
        }

        public HeartbeatEvent build() {
            return new HeartbeatEvent(
                    tenantId, tenantSlug, serverId, serverSlug, serverName, adapterVersion, uptime, enabledModules);
        }
    }
}
