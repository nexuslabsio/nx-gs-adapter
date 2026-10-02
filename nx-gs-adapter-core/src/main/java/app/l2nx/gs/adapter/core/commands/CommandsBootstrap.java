package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.rest.KafkaCredentials;
import app.l2nx.gs.adapter.api.rest.MessagingTopics;
import app.l2nx.gs.adapter.api.spi.capability.HostExecutor;
import app.l2nx.gs.adapter.api.spi.capability.NxCommands;
import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.adapter.api.spi.capability.NxSync;
import app.l2nx.gs.adapter.core.kafka.KafkaInitializer;
import app.l2nx.gs.adapter.core.kafka.gson.AdapterGson;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jspecify.annotations.Nullable;

/**
 * With no commands topic configured the {@link NxCommands} facade is still produced (so host
 * {@code ctx.commands().on(...)} calls succeed) but the consumer is {@code null}: commands disabled.
 */
public final class CommandsBootstrap {

    private static final NxLog log = NxLogFactory.getLogger(CommandsBootstrap.class);

    private CommandsBootstrap() {}

    /**
     * Rebuilds the consumer behind an existing facade so handlers registered before a reconnect survive.
     * Caller MUST stop the previous {@link CommandsConsumer} first.
     */
    public static @Nullable CommandsConsumer swap(
            NxCommands facade,
            @Nullable MessagingTopics messagingTopics,
            KafkaCredentials kafka,
            String clientIdBase,
            String groupId,
            UUID ownServerId,
            @Nullable Executor hostExecutor,
            Executor ioExecutor,
            NxEvents events,
            NxSync sync,
            DeferredReplies deferredReplies,
            CommandsConsumer.ReplySender replySender,
            @Nullable CommandsConfig config) {
        if (!(facade instanceof NxCommandsImpl)) {
            throw new IllegalArgumentException("swap() requires a facade produced by CommandsBootstrap.start(); got "
                    + (facade == null ? "null" : facade.getClass().getName()));
        }
        CommandTypeRegistry registry = ((NxCommandsImpl) facade).peekRegistry();
        if (registry == null) {
            registry = new CommandTypeRegistry();
            ((NxCommandsImpl) facade).swap(registry);
        }
        return buildConsumer(
                messagingTopics,
                kafka,
                clientIdBase,
                groupId,
                ownServerId,
                hostExecutor,
                ioExecutor,
                events,
                sync,
                deferredReplies,
                replySender,
                config,
                registry);
    }

    /**
     * @param groupId must sit under the tenant prefix ({@code <tenant>.gs.commands.<server>}) so the
     *                tenant principal's group ACL covers it
     * @param hostExecutor {@code null} if the host registered none; {@code ctx.host().sync(...)} then
     *                     throws {@link IllegalStateException}
     */
    public static Started start(
            @Nullable MessagingTopics messagingTopics,
            KafkaCredentials kafka,
            String clientIdBase,
            String groupId,
            UUID ownServerId,
            @Nullable Executor hostExecutor,
            Executor ioExecutor,
            NxEvents events,
            NxSync sync,
            DeferredReplies deferredReplies,
            CommandsConsumer.ReplySender replySender,
            @Nullable CommandsConfig config) {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        NxCommandsImpl commands = new NxCommandsImpl(registry);
        CommandsConsumer consumer = buildConsumer(
                messagingTopics,
                kafka,
                clientIdBase,
                groupId,
                ownServerId,
                hostExecutor,
                ioExecutor,
                events,
                sync,
                deferredReplies,
                replySender,
                config,
                registry);
        return new Started(commands, consumer);
    }

    private static @Nullable CommandsConsumer buildConsumer(
            @Nullable MessagingTopics messagingTopics,
            KafkaCredentials kafka,
            String clientIdBase,
            String groupId,
            UUID ownServerId,
            @Nullable Executor hostExecutor,
            Executor ioExecutor,
            NxEvents events,
            NxSync sync,
            DeferredReplies deferredReplies,
            CommandsConsumer.ReplySender replySender,
            @Nullable CommandsConfig config,
            CommandTypeRegistry registry) {
        String inboundTopic = (messagingTopics != null) ? messagingTopics.getCommandsTopic() : null;
        String repliesTopic = (messagingTopics != null) ? messagingTopics.getCommandsRepliesTopic() : null;

        if (inboundTopic == null || inboundTopic.isEmpty()) {
            log.info("Commands surface disabled — MessagingTopics.commandsTopic is unconfigured");
            return null;
        }

        if (repliesTopic == null || repliesTopic.isEmpty()) {
            log.warn(
                    "Commands inbound topic '{}' is configured but commandsRepliesTopic is not — "
                            + "handlers will run but replies will be dropped (web side will see timeouts)",
                    inboundTopic);
        }

        if (hostExecutor == null) {
            log.warn(
                    "commandsTopic '{}' is configured but no host executor registered — "
                            + "handlers requiring ctx.host().sync(...) will throw IllegalStateException. "
                            + "Call NxAdapter.hostExecutor(...) before NxAdapter.start()",
                    inboundTopic);
        }

        CommandsConfig effectiveConfig = (config != null) ? config : CommandsConfig.defaults();
        Map<String, Object> consumerProps = buildConsumerConfig(kafka, clientIdBase, groupId, effectiveConfig);
        Consumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(
                consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        Gson gson = AdapterGson.create();
        HostExecutor hostExec = new HostExecutorImpl(hostExecutor, effectiveConfig.getHostSyncTimeoutMs());

        CommandsConsumer consumer = new CommandsConsumer(
                inboundTopic,
                repliesTopic,
                ownServerId,
                hostExec,
                events,
                ioExecutor,
                sync,
                registry,
                deferredReplies,
                kafkaConsumer,
                replySender,
                gson,
                effectiveConfig);
        consumer.start();
        return consumer;
    }

    private static Map<String, Object> buildConsumerConfig(
            KafkaCredentials kafka, String clientIdBase, String groupId, CommandsConfig config) {
        String clientId = clientIdBase + "-commands";
        Map<String, Object> props = new LinkedHashMap<String, Object>();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.putAll(config.getKafkaOverrides());
        // pinned after overrides: security, identity and commit semantics must not be tunable
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrap());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("security.protocol", kafka.getSecurityProtocol());
        props.put("sasl.mechanism", kafka.getSaslMechanism());
        props.put("sasl.jaas.config", KafkaInitializer.buildJaas(kafka.getSaslUsername(), kafka.getSaslPassword()));
        return props;
    }

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
