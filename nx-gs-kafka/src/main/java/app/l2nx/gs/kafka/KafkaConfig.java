package app.l2nx.gs.kafka;

import com.google.gson.Gson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Immutable configuration for {@link NxKafka}. Created via {@link Builder}
 * obtained from {@link NxKafka#configure()}.
 */
public final class KafkaConfig {

    private final String brokers;
    private final String clientId;
    private final long connectTimeoutMs;
    private final boolean reconnect;
    private final long reconnectIntervalMs;
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

    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Static Kafka headers stamped on every record produced by this client.
     * Used to attach connection-scoped metadata (e.g. {@code Nx-Server-Id})
     * once at adapter bootstrap without modifying every per-call site.
     */
    public Map<String, byte[]> getProducerStaticHeaders() {
        return producerStaticHeaders;
    }

    public Gson getGson() {
        return gson;
    }

    public Consumer<KafkaState> getStateChangeListener() {
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
        private final Map<String, byte[]> producerStaticHeaders = new HashMap<>();
        private Gson gson = new Gson();
        private Consumer<KafkaState> stateChangeListener;

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
        public Builder onStateChange(Consumer<KafkaState> listener) {
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
         * Adds a static Kafka header stamped on every record produced by the
         * resulting client. Pre-encoded {@code byte[]} value is reused per record
         * — caller is responsible for the encoding (e.g. raw 16-byte UUID for
         * {@code Nx-Server-Id}).
         */
        public Builder producerStaticHeader(String name, byte[] value) {
            this.producerStaticHeaders.put(name, value);
            return this;
        }

        /**
         * Validates the configuration, connects to Kafka, and initializes the singleton.
         * If the broker is unreachable, the state is set to {@code DISCONNECTED}
         * (no exception thrown) and background reconnection starts if enabled.
         *
         * @return the initialized {@link NxKafka} singleton
         * @throws KafkaException if brokers are not set or NxKafka is already configured
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
            if (reconnectIntervalMs <= 0) {
                throw new KafkaException("Reconnect interval must be positive");
            }
            if (gson == null) {
                throw new KafkaException("Gson must not be null");
            }
            KafkaConfig config = new KafkaConfig(this);
            return NxKafka.initialize(config);
        }
    }
}
