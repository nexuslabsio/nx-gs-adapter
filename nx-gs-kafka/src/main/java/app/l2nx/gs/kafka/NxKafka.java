package app.l2nx.gs.kafka;

import app.l2nx.gs.kafka.consumer.NxConsumer;
import app.l2nx.gs.kafka.consumer.ReplyContext;
import app.l2nx.gs.kafka.producer.NxProducer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;

/** Singleton entry point; obtain a builder via {@link #configure()}. */
public final class NxKafka {

    private static volatile NxKafka instance;

    private final KafkaConfig config;
    private final NxLog log;
    private final NxProducer producer;
    private final Map<String, NxConsumer> consumers = new ConcurrentHashMap<>();
    private final Consumer<KafkaState> stateChangeListener;
    private final ScheduledExecutorService scheduler;
    private final Thread shutdownHook;
    private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile KafkaState state;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile AdminClient adminClient;

    private NxKafka(KafkaConfig config) {
        this.config = config;
        this.log = NxLogFactory.getLogger(NxKafka.class);
        this.state = KafkaState.CREATED;
        this.stateChangeListener = config.getStateChangeListener();

        shutdownHook = new Thread(this::doShutdown, "nx-gs-kafka-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        this.adminClient = createAdminClient();
        tryConnect();

        this.producer = createProducer();

        if (config.isReconnect()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "nx-gs-kafka-health");
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(
                        (thr, ex) -> log.error("Uncaught exception on scheduler thread {}", thr.getName(), ex));
                return t;
            });
            scheduler.scheduleWithFixedDelay(
                    this::healthCheck,
                    config.getReconnectIntervalMs(),
                    config.getReconnectIntervalMs(),
                    TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
        }

        log.debug(
                "NxKafka initialized, reconnect={}, interval={}ms",
                config.isReconnect(),
                config.getReconnectIntervalMs());
    }

    public static KafkaConfig.Builder configure() {
        return new KafkaConfig.Builder();
    }

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

    /** Fire-and-forget; delivery errors are logged, never thrown. */
    public void send(String topic, Object message) {
        if (closed.get()) {
            log.warn("Cannot send to {}: NxKafka is shut down", topic);
            return;
        }
        sendLock.readLock().lock();
        try {
            if (closed.get()) {
                log.warn("Cannot send to {}: NxKafka is shut down", topic);
                return;
            }
            producer.send(topic, message);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /** Same key lands in the same partition, preserving per-key order. */
    public void send(String topic, String key, Object message) {
        if (closed.get()) {
            log.warn("Cannot send to {}: NxKafka is shut down", topic);
            return;
        }
        sendLock.readLock().lock();
        try {
            if (closed.get()) {
                log.warn("Cannot send to {}: NxKafka is shut down", topic);
                return;
            }
            producer.send(topic, key, message);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /** Callback runs on the Kafka I/O thread. */
    public void send(String topic, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        sendLock.readLock().lock();
        try {
            if (rejectIfClosed(topic, callback)) {
                return;
            }
            producer.send(topic, message, callback);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    public void send(String topic, String key, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        sendLock.readLock().lock();
        try {
            if (rejectIfClosed(topic, callback)) {
                return;
            }
            producer.send(topic, key, message, callback);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /** Raw-bytes key; a null message sends a log-compaction tombstone. */
    public void send(String topic, byte[] key, Object message, Callback callback) {
        if (rejectIfClosed(topic, callback)) {
            return;
        }
        sendLock.readLock().lock();
        try {
            if (rejectIfClosed(topic, callback)) {
                return;
            }
            producer.send(topic, key, message, callback);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /** For records carrying per-record headers on top of the static producer headers. */
    public void sendBytesKeyRecord(
            org.apache.kafka.clients.producer.ProducerRecord<byte[], Object> record, Callback callback) {
        if (rejectIfClosed(record.topic(), callback)) {
            return;
        }
        sendLock.readLock().lock();
        try {
            if (rejectIfClosed(record.topic(), callback)) {
                return;
            }
            producer.sendBytesKeyRecord(record, callback);
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /** Fails the callback and returns true when closed; the caller must then not send. */
    private boolean rejectIfClosed(String topic, Callback callback) {
        if (!closed.get()) {
            return false;
        }
        log.warn("Cannot send to {}: NxKafka is shut down", topic);
        try {
            callback.onCompletion(null, new KafkaException("NxKafka is shut down"));
        } catch (Exception e) {
            log.error("Callback error for topic {}", topic, e);
        }
        return true;
    }

    /** One daemon poll thread per topic; throws if already subscribed or shut down. */
    public <T> void subscribe(String topic, String groupId, Class<T> type, Consumer<T> handler) {
        subscribe(topic, groupId, type, (message, replyTo) -> handler.accept(message));
    }

    /** As above; the handler answers the requester via {@link ReplyContext#reply(Object)}. */
    public <T> void subscribe(String topic, String groupId, Class<T> type, BiConsumer<T, ReplyContext> handler) {
        if (groupId == null || groupId.isEmpty()) {
            throw new KafkaException("groupId must be non-empty");
        }
        if (closed.get()) {
            throw new KafkaException("Cannot subscribe: NxKafka is shut down");
        }
        Map<String, Object> consumerConfig = createConsumerConfig(topic, groupId);
        NxConsumer group = NxConsumer.create(topic, type, handler, producer, config.getGson(), consumerConfig);
        if (consumers.putIfAbsent(topic, group) != null) {
            group.stop();
            throw new KafkaException("Already subscribed to topic: " + topic);
        }
        log.info("Subscribed to topic {} with groupId {}", topic, groupId);
    }

    public void unsubscribe(String topic) {
        NxConsumer group = consumers.remove(topic);
        if (group != null) {
            group.stop();
            log.info("Unsubscribed from topic {}", topic);
        }
    }

    /** Blocks until buffered records reach the broker; takes the send read-lock so it cannot race close(). */
    public void flush() {
        if (closed.get()) {
            return;
        }
        sendLock.readLock().lock();
        try {
            if (closed.get()) {
                return;
            }
            producer.flush();
        } finally {
            sendLock.readLock().unlock();
        }
    }

    public boolean isConnected() {
        return state == KafkaState.CONNECTED;
    }

    public KafkaState state() {
        return state;
    }

    /** Idempotent; also runs as a JVM shutdown hook. Concurrent callers wait for the first to finish. */
    public void shutdown() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // JVM already shutting down; the hook is firing or has fired
        }
        doShutdown();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void doShutdown() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            sendLock.writeLock().lock();
            try {
                changeState(KafkaState.CLOSED);

                if (scheduler != null) {
                    scheduler.shutdownNow();
                }
                for (NxConsumer group : consumers.values()) {
                    group.stop();
                }
                consumers.clear();
                closeAdminClient();
                if (producer != null) {
                    producer.close();
                }

                log.info("NxKafka shut down");
                instance = null;
            } finally {
                sendLock.writeLock().unlock();
            }
        } finally {
            shutdownLatch.countDown();
        }
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
                log.error("State change listener error", e);
            }
        }
    }

    private Map<String, Object> createConsumerConfig(String topic, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.putAll(config.getProperties());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getClientId() + "-consumer-" + topic);
        return props;
    }

    private NxProducer createProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.putAll(config.getProperties());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getClientId() + "-producer");
        return NxProducer.create(
                props, config.getGson(), config.getProducerStaticHeaders(), config.getProducerCloseTimeout());
    }

    private AdminClient createAdminClient() {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, config.getClientId() + "-admin");
        adminConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) config.getConnectTimeoutMs());
        adminConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) config.getConnectTimeoutMs());
        adminConfig.putAll(config.getProperties());
        try {
            return AdminClient.create(adminConfig);
        } catch (Exception e) {
            log.warn("Failed to create AdminClient for {}", config.getBrokers(), e);
            return null;
        }
    }

    private void closeAdminClient() {
        AdminClient local = adminClient;
        if (local == null) {
            return;
        }
        adminClient = null;
        try {
            local.close();
        } catch (Exception e) {
            log.warn("Error closing AdminClient", e);
        }
    }

    private void tryConnect() {
        if (closed.get()) {
            return;
        }
        if (adminClient == null) {
            adminClient = createAdminClient();
            if (adminClient == null) {
                changeState(KafkaState.DISCONNECTED);
                return;
            }
        }
        try {
            DescribeClusterResult result = adminClient.describeCluster();
            String clusterId = result.clusterId().get(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
            int brokerCount = result.nodes()
                    .get(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                    .size();

            if (!closed.get()) {
                changeState(KafkaState.CONNECTED);
                log.info("Connected to cluster {}, brokers: {}", clusterId, brokerCount);
            }
        } catch (Exception e) {
            if (!closed.get()) {
                changeState(KafkaState.DISCONNECTED);
                log.warn("Failed to connect to Kafka at {}", config.getBrokers(), e);
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
