package app.l2nx.gs.adapter.api.kafka;

import java.util.Objects;

/**
 * Heartbeat message published every 60s. Kafka message key is {@code serverId}.
 *
 * <p>{@code uptime} is seconds since the most recent successful {@code /connect}
 * (session-scoped — resets on reconnect).</p>
 */
public final class HeartbeatEvent {

    private final String serverId;
    private final String adapterVersion;
    private final long uptime;

    public HeartbeatEvent(String serverId, String adapterVersion, long uptime) {
        this.serverId = serverId;
        this.adapterVersion = adapterVersion;
        this.uptime = uptime;
    }

    public String getServerId() {
        return serverId;
    }

    public String getAdapterVersion() {
        return adapterVersion;
    }

    public long getUptime() {
        return uptime;
    }

    public Builder toBuilder() {
        return new Builder()
                .serverId(serverId)
                .adapterVersion(adapterVersion)
                .uptime(uptime);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HeartbeatEvent)) return false;
        HeartbeatEvent that = (HeartbeatEvent) o;
        return uptime == that.uptime
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(adapterVersion, that.adapterVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, adapterVersion, uptime);
    }

    @Override
    public String toString() {
        return "HeartbeatEvent[serverId=" + serverId
                + ", adapterVersion=" + adapterVersion
                + ", uptime=" + uptime + "]";
    }

    public static final class Builder {
        private String serverId;
        private String adapterVersion;
        private long uptime;

        public Builder serverId(String serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder adapterVersion(String adapterVersion) {
            this.adapterVersion = adapterVersion;
            return this;
        }

        public Builder uptime(long uptime) {
            this.uptime = uptime;
            return this;
        }

        public HeartbeatEvent build() {
            return new HeartbeatEvent(serverId, adapterVersion, uptime);
        }
    }
}
