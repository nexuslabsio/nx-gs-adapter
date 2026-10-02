package app.l2nx.gs.kafka;

import com.google.gson.Gson;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class KafkaConfig {

    private final String brokers;
    private final String clientId;
    private final long connectTimeoutMs;
    private final boolean reconnect;
    private final long reconnectIntervalMs;
    private final Duration producerCloseTimeout;
    private final Map<String, Object> properties;
    private final Map<String, byte[]> producerStaticHeaders;
    private final Gson gson;
    private final Consumer<KafkaState> stateChangeListener;

    private KafkaConfig(Builder builder) {
        this.brokers = builder.brokers;
        this.clientId = builder.clientId;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.reconnect = builder.reconnect;
        this.reconnectIntervalMs = builder.reconnectIntervalMs;
        this.producerCloseTimeout = builder.producerCloseTimeout;
        this.properties = Collections.unmodifiableMap(new HashMap<>(builder.properties));
        this.producerStaticHeaders = Collections.unmodifiableMap(new HashMap<>(builder.producerStaticHeaders));
        this.gson = builder.gson;
        this.stateChangeListener = builder.stateChangeListener;
    }

    public String getBrokers() {
        return brokers;
    }

    public String getClientId() {
        return clientId;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public long getReconnectIntervalMs() {
        return reconnectIntervalMs;
    }

    public Duration getProducerCloseTimeout() {
        return producerCloseTimeout;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Map<String, byte[]> getProducerStaticHeaders() {
        return producerStaticHeaders;
    }

    public Gson getGson() {
        return gson;
    }

    public Consumer<KafkaState> getStateChangeListener() {
        return stateChangeListener;
    }

    public static final class Builder {

        private String brokers;
        private String clientId = "nx-gs-kafka";
        private long connectTimeoutMs = 5000;
        private boolean reconnect = true;
        private long reconnectIntervalMs = 30000;
        private Duration producerCloseTimeout = Duration.ofSeconds(10);
        private final Map<String, Object> properties = new HashMap<>();
        private final Map<String, byte[]> producerStaticHeaders = new HashMap<>();
        private Gson gson = new Gson();
        private Consumer<KafkaState> stateChangeListener;

        Builder() {}

        public Builder brokers(String brokers) {
            this.brokers = brokers;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder connectTimeout(long timeout, TimeUnit unit) {
            this.connectTimeoutMs = unit.toMillis(timeout);
            return this;
        }

        public Builder reconnect(boolean reconnect) {
            this.reconnect = reconnect;
            return this;
        }

        public Builder reconnectInterval(long interval, TimeUnit unit) {
            this.reconnectIntervalMs = unit.toMillis(interval);
            return this;
        }

        /**
         * Must be >= l2nx.events.shutdown-drain-timeout-ms: this close is what flushes
         * fire-and-forget replies and events at shutdown, otherwise they are truncated.
         */
        public Builder producerCloseTimeout(Duration timeout) {
            this.producerCloseTimeout = timeout;
            return this;
        }

        public Builder gson(Gson gson) {
            this.gson = gson;
            return this;
        }

        /** Runs on the health-check thread; hand off to the game thread if needed. */
        public Builder onStateChange(Consumer<KafkaState> listener) {
            this.stateChangeListener = listener;
            return this;
        }

        /** Raw Kafka property for AdminClient, producer and consumers; overrides library defaults. */
        public Builder property(String key, Object value) {
            this.properties.put(key, value);
            return this;
        }

        /** Value bytes are shared across records; the caller encodes them and must not mutate. */
        public Builder producerStaticHeader(String name, byte[] value) {
            this.producerStaticHeaders.put(name, value);
            return this;
        }

        /**
         * Does not throw when brokers are unreachable: state becomes DISCONNECTED and reconnect starts if enabled.
         */
        public NxKafka build() {
            if (brokers == null || brokers.trim().isEmpty()) {
                throw new KafkaException("Brokers must be specified");
            }
            if (clientId == null || clientId.trim().isEmpty()) {
                throw new KafkaException("Client ID must not be blank");
            }
            if (connectTimeoutMs <= 0) {
                throw new KafkaException("Connect timeout must be positive");
            }
            if (connectTimeoutMs > 60_000) {
                throw new KafkaException("Connect timeout must not exceed 60s");
            }
            if (reconnectIntervalMs <= 0) {
                throw new KafkaException("Reconnect interval must be positive");
            }
            if (reconnectIntervalMs > 5L * 60L * 1000L) {
                throw new KafkaException("Reconnect interval must not exceed 5min");
            }
            if (producerCloseTimeout == null || producerCloseTimeout.isNegative() || producerCloseTimeout.isZero()) {
                throw new KafkaException("Producer close timeout must be positive");
            }
            if (gson == null) {
                throw new KafkaException("Gson must not be null");
            }
            KafkaConfig config = new KafkaConfig(this);
            return NxKafka.initialize(config);
        }
    }
}
