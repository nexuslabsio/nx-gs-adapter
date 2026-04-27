package app.l2nx.gs.kafka;

import com.google.gson.Gson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Immutable configuration for NxKafka. Created via {@link Builder}
 * obtained from {@link NxKafka#configure()}.
 */
public final class NxKafkaConfig {

    private final String brokers;
    private final String clientId;
    private final long connectTimeoutMs;
    private final boolean reconnect;
    private final long reconnectIntervalMs;
    private final Map<String, Object> properties;
    private final Gson gson;
    private final Consumer<NxKafkaState> stateChangeListener;

    private NxKafkaConfig(Builder builder) {
        this.brokers = builder.brokers;
        this.clientId = builder.clientId;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.reconnect = builder.reconnect;
        this.reconnectIntervalMs = builder.reconnectIntervalMs;
        this.properties = Collections.unmodifiableMap(new HashMap<>(builder.properties));
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

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Gson getGson() {
        return gson;
    }

    public Consumer<NxKafkaState> getStateChangeListener() {
        return stateChangeListener;
    }

    /**
     * Fluent builder for NxKafka configuration.
     *
     * <pre>{@code
     * NxKafka kafka = NxKafka.configure()
     *     .brokers("kafka1:9092,kafka2:9092")
     *     .clientId("bohpts-x20")
     *     .connectTimeout(5, TimeUnit.SECONDS)
     *     .reconnect(true)
     *     .reconnectInterval(30, TimeUnit.SECONDS)
     *     .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd").create())
     *     .onStateChange(state -> log.info("Kafka: {}", state))
     *     .property("security.protocol", "PLAINTEXT")
     *     .build();
     * }</pre>
     */
    public static final class Builder {

        private String brokers;
        private String clientId = "nx-gs-kafka";
        private long connectTimeoutMs = 5000;
        private boolean reconnect = true;
        private long reconnectIntervalMs = 30000;
        private final Map<String, Object> properties = new HashMap<>();
        private Gson gson = new Gson();
        private Consumer<NxKafkaState> stateChangeListener;

        Builder() {
        }

        /**
         * Kafka bootstrap servers (required). Comma-separated, e.g. {@code "kafka1:9092,kafka2:9092"}.
         */
        public Builder brokers(String brokers) {
            this.brokers = brokers;
            return this;
        }

        /**
         * Kafka client identifier, used in broker logs. Default: {@code "nx-gs-kafka"}.
         */
        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Timeout for the initial connection check via AdminClient. Default: 5 seconds.
         */
        public Builder connectTimeout(long timeout, TimeUnit unit) {
            this.connectTimeoutMs = unit.toMillis(timeout);
            return this;
        }

        /**
         * Enable background health-check and automatic reconnection. Default: {@code true}.
         */
        public Builder reconnect(boolean reconnect) {
            this.reconnect = reconnect;
            return this;
        }

        /**
         * Interval between background health checks. Default: 30 seconds.
         */
        public Builder reconnectInterval(long interval, TimeUnit unit) {
            this.reconnectIntervalMs = unit.toMillis(interval);
            return this;
        }

        /**
         * Custom Gson instance for JSON serialization/deserialization. Default: {@code new Gson()}.
         */
        public Builder gson(Gson gson) {
            this.gson = gson;
            return this;
        }

        /**
         * Callback invoked when connection state changes (e.g. CONNECTED → DISCONNECTED).
         * Called on the health-check thread — dispatch to game thread if needed.
         */
        public Builder onStateChange(Consumer<NxKafkaState> listener) {
            this.stateChangeListener = listener;
            return this;
        }

        /**
         * Sets a raw Kafka client property passed to both AdminClient and Producer.
         */
        public Builder property(String key, Object value) {
            this.properties.put(key, value);
            return this;
        }

        /**
         * Validates the configuration, connects to Kafka, and initializes the singleton.
         * If the broker is unreachable, the state is set to {@code DISCONNECTED}
         * (no exception thrown) and background reconnection starts if enabled.
         *
         * @return the initialized {@link NxKafka} singleton
         * @throws NxKafkaException if brokers are not set or NxKafka is already configured
         */
        public NxKafka build() {
            if (brokers == null || brokers.trim().isEmpty()) {
                throw new NxKafkaException("Brokers must be specified");
            }
            if (clientId == null || clientId.trim().isEmpty()) {
                throw new NxKafkaException("Client ID must not be blank");
            }
            if (connectTimeoutMs <= 0) {
                throw new NxKafkaException("Connect timeout must be positive");
            }
            if (reconnectIntervalMs <= 0) {
                throw new NxKafkaException("Reconnect interval must be positive");
            }
            if (gson == null) {
                throw new NxKafkaException("Gson must not be null");
            }
            NxKafkaConfig config = new NxKafkaConfig(this);
            return NxKafka.initialize(config);
        }
    }
}
