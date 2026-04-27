package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;

/**
 * Kafka topic addressing returned in {@link KafkaConfig}.
 *
 * <p>Topic names are populated by the producer of {@link ConnectResponse}; consumers use
 * them verbatim and never derive topic names locally.</p>
 */
public final class Topics {

    private final String heartbeat;

    public Topics(String heartbeat) {
        this.heartbeat = heartbeat;
    }

    public String getHeartbeat() {
        return heartbeat;
    }

    public Builder toBuilder() {
        return new Builder().heartbeat(heartbeat);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topics)) return false;
        Topics that = (Topics) o;
        return Objects.equals(heartbeat, that.heartbeat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(heartbeat);
    }

    @Override
    public String toString() {
        return "Topics[heartbeat=" + heartbeat + "]";
    }

    public static final class Builder {
        private String heartbeat;

        public Builder heartbeat(String heartbeat) {
            this.heartbeat = heartbeat;
            return this;
        }

        public Topics build() {
            return new Topics(heartbeat);
        }
    }
}
