package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.rest.KafkaConfig;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.spi.HostExecutor;
import app.l2nx.gs.adapter.api.spi.NxCommands;
import app.l2nx.gs.adapter.api.spi.NxEvents;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Public factory for the commands consume subsystem. {@code NxAdapter}
 * calls {@link #start} once per connect cycle to wire up the registry +
 * Kafka consumer + daemon thread, returning a {@link Started} bundle
 * carrying the {@link CommandsConsumer} (for shutdown + heartbeat) and the
 * {@link NxCommands} façade (for {@code ConnectContext.commands()}).
 *
 * <p>Hides {@code CommandTypeRegistry}, {@code NxCommandsImpl},
 * {@code CommandContextImpl}, and {@code HostExecutorImpl} — those are
 * package-private implementation details. Callers depend only on the public
 * {@link NxCommands} interface and the {@link CommandsConsumer} class.</p>
 *
 * <p>When {@link MessagingTopics#getCommandsTopic()} is unconfigured, this
 * factory still produces an {@link NxCommands} façade (so host code's
 * unconditional {@code ctx.commands().on(...)} calls succeed) but DOES NOT
 * spawn a Kafka consumer thread. The {@link CommandsConsumer} returned in
 * that case is {@code null} — the caller (i.e. {@code NxAdapter}) treats
 * that as "commands disabled" for heartbeat purposes.</p>
 */
public final class CommandsBootstrap {

    private static final NxLog log = NxLogFactory.getLogger(CommandsBootstrap.class);

    private CommandsBootstrap() {
    }

    /**
     * Wire up the registry, the {@link NxCommands} façade, and (when
     * {@code messagingTopics.commandsTopic} is configured) a Kafka consumer
     * + daemon thread. Returns a {@link Started} bundle.
     *
     * @param messagingTopics the platform-issued addressing bundle; may be
     *                        {@code null} (treated as commands disabled).
     * @param kafka           platform-issued Kafka config; provides brokers,
     *                        SASL credentials, etc.
     * @param clientIdBase    base client id (e.g.
     *                        {@code nx-gs-adapter-<tenant>-<server>}); the
     *                        commands consumer appends {@code -commands}.
     * @param hostExecutor    host's game-side {@link Executor}; may be
     *                        {@code null} when host code has not registered
     *                        one — handlers requiring
     *                        {@code ctx.host().sync(...)} will then throw
     *                        {@link IllegalStateException} on first hop.
     * @param events          the {@link NxEvents} façade so handlers can
     *                        publish side-effect events.
     * @param replySender     the bridge to the actual Kafka send (production
     *                        wires this to {@code NxKafka.sendBytesKeyRecord}).
     * @param config          operator-tunable knobs; falls back to
     *                        {@link CommandsConfig#defaults()} when {@code null}.
     */
    public static Started start(@Nullable MessagingTopics messagingTopics,
                                KafkaConfig kafka,
                                String clientIdBase,
                                @Nullable Executor hostExecutor,
                                NxEvents events,
                                CommandsConsumer.ReplySender replySender,
                                @Nullable CommandsConfig config) {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        NxCommandsImpl commands = new NxCommandsImpl(registry);

        String inboundTopic = (messagingTopics != null) ? messagingTopics.getCommandsTopic() : null;
        String repliesTopic = (messagingTopics != null) ? messagingTopics.getCommandsRepliesTopic() : null;

        if (inboundTopic == null || inboundTopic.isEmpty()) {
            log.info("Commands surface disabled — MessagingTopics.commandsTopic is unconfigured");
            return new Started(commands, null);
        }

        if (repliesTopic == null || repliesTopic.isEmpty()) {
            log.warn("Commands inbound topic '{}' is configured but commandsRepliesTopic is not — "
                            + "handlers will run but replies will be dropped (web side will see timeouts)",
                    inboundTopic);
        }

        if (hostExecutor == null) {
            log.warn("commandsTopic '{}' is configured but no host executor registered — "
                    + "handlers requiring ctx.host().sync(...) will throw IllegalStateException. "
                    + "Call NxAdapter.hostExecutor(...) before NxAdapter.start()", inboundTopic);
        }

        CommandsConfig effectiveConfig = (config != null) ? config : CommandsConfig.defaults();
        Map<String, Object> consumerProps = buildConsumerConfig(kafka, clientIdBase, effectiveConfig);
        Consumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(
                consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        HostExecutor hostExec = new HostExecutorImpl(hostExecutor, effectiveConfig.getHostSyncTimeoutMs());

        CommandsConsumer consumer = new CommandsConsumer(
                inboundTopic,
                repliesTopic,
                hostExec,
                events,
                registry,
                kafkaConsumer,
                replySender,
                gson,
                effectiveConfig);
        consumer.start();
        return new Started(commands, consumer);
    }

    private static Map<String, Object> buildConsumerConfig(KafkaConfig kafka,
                                                           String clientIdBase,
                                                           CommandsConfig config) {
        String clientId = clientIdBase + "-commands";
        Map<String, Object> props = new LinkedHashMap<String, Object>();
        // Internal defaults (overridable via l2nx.commands.kafka.*)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        // Operator overrides
        props.putAll(config.getKafkaOverrides());
        // Hard-pinned (security + identity + commit semantics)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrap());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("security.protocol", kafka.getSecurityProtocol());
        props.put("sasl.mechanism", kafka.getSaslMechanism());
        props.put("sasl.jaas.config",
                KafkaInitializer.buildJaas(kafka.getSaslUsername(), kafka.getSaslPassword()));
        return props;
    }

    /**
     * Tuple of the {@link NxCommands} façade and the optional
     * {@link CommandsConsumer}. {@code consumer()} is {@code null} when
     * commands are disabled (no inbound topic).
     */
    public static final class Started {

        private final NxCommands commands;
        private final @Nullable CommandsConsumer consumer;

        Started(NxCommands commands, @Nullable CommandsConsumer consumer) {
            this.commands = commands;
            this.consumer = consumer;
        }

        public NxCommands commands() {
            return commands;
        }

        public @Nullable CommandsConsumer consumer() {
            return consumer;
        }
    }
}
