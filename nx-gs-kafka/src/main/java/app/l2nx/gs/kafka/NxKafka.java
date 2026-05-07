package app.l2nx.gs.kafka;

import app.l2nx.gs.kafka.consumer.NxConsumer;
import app.l2nx.gs.kafka.consumer.ReplyContext;
import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Singleton entry point for NxKafka library.
 *
 * <pre>{@code
 * // Configure and connect
 * NxKafka kafka = NxKafka.configure()
 *     .brokers("kafka1:9092,kafka2:9092")
 *     .clientId("bohpts-x20")
 *     .reconnect(true)
 *     .build();
 *
 * // Send messages
 * kafka.send("bohpts.x20.purchased", new PurchaseEvent(playerId, itemId, price));
 *
 * // Subscribe to messages
 * kafka.subscribe("bohpts.x20.purchased", PurchaseEvent.class, event -> {
 *     gameServer.enqueue(() -> handleEvent(event));
 * });
 *
 * // Access from anywhere
 * NxKafka.instance().isConnected();
 *
 * // Shutdown (also registered as JVM shutdown hook)
 * NxKafka.instance().shutdown();
 * }</pre>
 */
public final class NxKafka {

    private static volatile NxKafka instance;

    private final KafkaConfig config;
    private final NxLog log;
    private final NxProducer producer;
    private final Map<String, NxConsumer> consumers = new ConcurrentHashMap<>();
    private final Consumer<KafkaState> stateChangeListener;
    private final ScheduledExecutorService scheduler;
    private final Thread shutdownHook;

    private volatile KafkaState state;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private NxKafka(KafkaConfig config) {
        this.config = config;
        this.log = NxLogFactory.getLogger(NxKafka.class);
        this.state = KafkaState.CREATED;
        this.stateChangeListener = config.getStateChangeListener();

        // Register JVM shutdown hook BEFORE starting scheduler
        shutdownHook = new Thread(this::doShutdown, "nx-gs-kafka-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // Initial connection attempt
        tryConnect();

        // Create producer (KafkaProducer handles retries internally, works even when disconnected)
        this.producer = createProducer();

        // Start background health check scheduler
        if (config.isReconnect()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "nx-gs-kafka-health");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(
                    this::healthCheck,
                    config.getReconnectIntervalMs(),
                    config.getReconnectIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            scheduler = null;
        }

        log.debug("NxKafka initialized, reconnect={}, interval={}ms",
                config.isReconnect(), config.getReconnectIntervalMs());
    }

    /**
     * Creates a new configuration builder. Call {@link KafkaConfig.Builder#build()} to
     * initialize the singleton and connect to the Kafka cluster.
     *
     * @return configuration builder
     * @throws KafkaException if already configured and not shut down
     */
    public static KafkaConfig.Builder configure() {
        return new KafkaConfig.Builder();
    }

    /**
     * Returns the singleton instance.
     *
     * @return the configured NxKafka instance
     * @throws KafkaException if {@link #configure()} has not been called yet
     */
    public static NxKafka instance() {
        NxKafka local = instance;
        if (local == null) {
            throw new KafkaException("NxKafka not configured. Call NxKafka.configure().build() first");
        }
        return local;
    }

    static NxKafka initialize(KafkaConfig config) {
        synchronized (NxKafka.class) {
            if (instance != null && instance.state() != KafkaState.CLOSED) {
                throw new KafkaException("NxKafka already configured. Call shutdown() first");
            }
            NxKafka kafka = new NxKafka(config);
            instance = kafka;
            return kafka;
        }
    }

    /**
     * Sends a message to the specified topic (fire-and-forget).
     * The message is serialized to JSON via Gson. Delivery errors are logged
     * internally and never propagated to the calling thread.
     *
     * @param topic   Kafka topic name
     * @param message object to serialize as JSON; must be Gson-serializable
     */
    public void send(String topic, Object message) {
        if (closed.get()) {
            log.warn("Cannot send to {}: NxKafka is shut down", topic);
            return;
        }
        producer.send(topic, message);
    }

    /**
     * Sends a keyed message to the specified topic (fire-and-forget).
     * Messages with the same key are guaranteed to land in the same partition,
     * preserving ordering for related events (e.g. per-player).
     *
     * @param topic   Kafka topic name
     * @param key     partition key (e.g. player ID); may be null for round-robin
     * @param message object to serialize as JSON; must be Gson-serializable
     */
    public void send(String topic, String key, Object message) {
        if (closed.get()) {
            log.warn("Cannot send to {}: NxKafka is shut down", topic);
            return;
        }
        producer.send(topic, key, message);
    }

    /**
     * Sends a message to the specified topic with a delivery callback.
     * The message is serialized to JSON via Gson. The callback is invoked
     * on the Kafka I/O thread when the broker acknowledges (or rejects) the record.
     *
     * <pre>{@code
     * kafka.send("events.purchase", event, (metadata, exception) -> {
     *     if (exception != null) log.warn("Send failed", exception);
     * });
     * }</pre>
     *
     * @param topic    Kafka topic name
     * @param message  object to serialize as JSON; must be Gson-serializable
     * @param callback invoked with {@link org.apache.kafka.clients.producer.RecordMetadata}
     *                 on success, or with an exception on failure; never null
     */
    public void send(String topic, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        producer.send(topic, message, callback);
    }

    /**
     * Sends a keyed message to the specified topic with a delivery callback.
     * Messages with the same key are guaranteed to land in the same partition.
     *
     * @param topic    Kafka topic name
     * @param key      partition key (e.g. player ID); may be null for round-robin
     * @param message  object to serialize as JSON; must be Gson-serializable
     * @param callback invoked with {@link org.apache.kafka.clients.producer.RecordMetadata}
     *                 on success, or with an exception on failure; never null
     */
    public void send(String topic, String key, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        producer.send(topic, key, message, callback);
    }

    /**
     * Sends a byte-array-keyed message with a delivery callback. Used when the
     * partition key is raw bytes rather than a UTF-8 string — primitive-PK
     * CDC keying, binary correlation IDs, etc. Same partition guarantee as the
     * String-keyed overload (partitioning is on the raw key bytes either way).
     *
     * @param topic    Kafka topic name
     * @param key      raw partition key bytes; may be null for round-robin
     * @param message  object to serialize as JSON; null for log-compaction tombstones
     * @param callback invoked with {@link org.apache.kafka.clients.producer.RecordMetadata}
     *                 on success, or with an exception on failure; never null
     */
    public void send(String topic, byte[] key, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        producer.send(topic, key, message, callback);
    }

    /**
     * Sends a pre-built byte-array-keyed producer record with a delivery callback.
     * Use when the caller needs to attach per-record headers (e.g.
     * {@code Nx-Message-Type}) in addition to the producer's static headers.
     *
     * @param record   pre-built record carrying topic + key + headers + value
     * @param callback invoked on the Kafka I/O thread when the broker acknowledges or rejects the record
     */
    public void sendBytesKeyRecord(org.apache.kafka.clients.producer.ProducerRecord<byte[], Object> record,
                                   Callback callback) {
        if (rejectIfClosed(record.topic(), callback)) {
            return;
        }
        producer.sendBytesKeyRecord(record, callback);
    }

    /**
     * Shared close-state guard for callback-flavored sends. Returns {@code true}
     * when the call has been rejected (caller MUST return without sending);
     * fires the failure callback with a "NxKafka is shut down" exception.
     */
    private boolean rejectIfClosed(String topic, Callback callback) {
        if (!closed.get()) {
            return false;
        }
        log.warn("Cannot send to {}: NxKafka is shut down", topic);
        try {
            callback.onCompletion(null, new KafkaException("NxKafka is shut down"));
        } catch (Exception e) {
            log.error("Callback error for topic {}: {}", topic, e.getMessage());
        }
        return true;
    }

    /**
     * Subscribes to a topic with a typed message handler (fire-and-forget messages).
     * Creates a dedicated daemon thread with a poll loop for this topic.
     *
     * <pre>{@code
     * kafka.subscribe("bohpts.x20.purchased", PurchaseEvent.class, event -> {
     *     gameServer.enqueue(() -> shop.handlePurchase(event));
     * });
     * }</pre>
     *
     * @param topic   Kafka topic name
     * @param type    message class for Gson deserialization
     * @param handler invoked for each message on the consumer thread
     * @param <T>     message type
     * @throws KafkaException if already subscribed to this topic or NxKafka is shut down
     */
    public <T> void subscribe(String topic, Class<T> type, Consumer<T> handler) {
        subscribe(topic, type, (message, replyTo) -> handler.accept(message));
    }

    /**
     * Subscribes to a topic with a typed message handler and reply support.
     * Creates a dedicated daemon thread with a poll loop for this topic.
     * Use {@link ReplyContext#reply(Object)} to send responses back to the requester.
     *
     * <pre>{@code
     * kafka.subscribe("gs.char.info.request", CharInfoRequest.class, (request, replyTo) -> {
     *     CharInfo info = gameServer.getCharInfo(request.getCharId());
     *     replyTo.reply(info);
     * });
     * }</pre>
     *
     * @param topic   Kafka topic name
     * @param type    message class for Gson deserialization
     * @param handler invoked for each message with a {@link ReplyContext} on the consumer thread
     * @param <T>     message type
     * @throws KafkaException if already subscribed to this topic or NxKafka is shut down
     */
    public <T> void subscribe(String topic, Class<T> type, BiConsumer<T, ReplyContext> handler) {
        if (closed.get()) {
            throw new KafkaException("Cannot subscribe: NxKafka is shut down");
        }
        Map<String, Object> consumerConfig = createConsumerConfig(topic);
        NxConsumer group = NxConsumer.create(topic, type, handler, producer, config.getGson(), consumerConfig);
        if (consumers.putIfAbsent(topic, group) != null) {
            group.stop();
            throw new KafkaException("Already subscribed to topic: " + topic);
        }
        log.info("Subscribed to topic {}", topic);
    }

    /**
     * Unsubscribes from a topic, stopping its poll thread and closing the consumer.
     *
     * @param topic Kafka topic name
     */
    public void unsubscribe(String topic) {
        NxConsumer group = consumers.remove(topic);
        if (group != null) {
            group.stop();
            log.info("Unsubscribed from topic {}", topic);
        }
    }

    public boolean isConnected() {
        return state == KafkaState.CONNECTED;
    }

    public KafkaState state() {
        return state;
    }

    /**
     * Gracefully shuts down NxKafka: stops all consumer poll threads,
     * stops the health-check scheduler, closes the producer, and clears
     * the singleton instance.
     * Also registered as a JVM shutdown hook, so explicit calls are optional.
     * Safe to call multiple times.
     */
    public void shutdown() {
        if (closed.get()) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // JVM is already shutting down
        }
        doShutdown();
    }

    private void doShutdown() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        changeState(KafkaState.CLOSED);

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        for (NxConsumer group : consumers.values()) {
            group.stop();
        }
        consumers.clear();
        producer.close();

        log.info("NxKafka shut down");
        instance = null;
    }

    private void changeState(KafkaState newState) {
        KafkaState oldState = this.state;
        if (oldState == newState) {
            return;
        }
        this.state = newState;
        if (stateChangeListener != null) {
            try {
                stateChangeListener.accept(newState);
            } catch (Exception e) {
                log.error("State change listener error: {}", e.getMessage());
            }
        }
    }

    private Map<String, Object> createConsumerConfig(String topic) {
        Map<String, Object> props = new HashMap<>();
        // Defaults — overridable by user properties
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.putAll(config.getProperties());
        // Internal settings — not overridable
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getClientId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getClientId() + "-consumer-" + topic);
        return props;
    }

    private NxProducer createProducer() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(config.getProperties());
        // Internal settings — not overridable
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getClientId() + "-producer");
        return NxProducer.create(props, config.getGson(), config.getProducerStaticHeaders());
    }

    private void tryConnect() {
        if (closed.get()) {
            return;
        }

        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, config.getClientId() + "-admin");
        adminConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) config.getConnectTimeoutMs());
        adminConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) config.getConnectTimeoutMs());
        adminConfig.putAll(config.getProperties());

        try (AdminClient admin = AdminClient.create(adminConfig)) {
            DescribeClusterResult result = admin.describeCluster();
            String clusterId = result.clusterId().get(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
            int brokerCount = result.nodes().get(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS).size();

            if (!closed.get()) {
                changeState(KafkaState.CONNECTED);
                log.info("Connected to cluster {}, brokers: {}", clusterId, brokerCount);
            }
        } catch (Exception e) {
            if (!closed.get()) {
                changeState(KafkaState.DISCONNECTED);
                log.warn("Failed to connect to Kafka at {}: {}", config.getBrokers(), e.getMessage(), e);
            }
        }
    }

    private void healthCheck() {
        if (closed.get()) {
            return;
        }
        KafkaState previousState = state;
        tryConnect();

        if (closed.get()) {
            return;
        }
        if (state == KafkaState.CONNECTED && previousState == KafkaState.DISCONNECTED) {
            log.info("Reconnected to Kafka");
        } else if (state == KafkaState.DISCONNECTED && previousState == KafkaState.CONNECTED) {
            log.warn("Lost connection to Kafka");
        }
    }
}
