package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.spi.CommandHandler;
import app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException;
import app.l2nx.gs.adapter.api.spi.NxEvents;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CommandsConsumerTest {

    static final class FakeCommand implements NxCommand<Void> {
        Long charId;

        public Long getCharId() {
            return charId;
        }
    }

    /**
     * Captures every reply record sent and synchronously invokes its callback
     * so {@code pendingReplies} drains. Callback is invoked with success
     * metadata by default; tests that need failure semantics can swap.
     */
    static final class CapturingReplySender implements CommandsConsumer.ReplySender {
        final List<ProducerRecord<byte[], Object>> sent = new ArrayList<>();
        boolean simulateFailure = false;

        @Override
        public void send(ProducerRecord<byte[], Object> record, Callback callback) {
            sent.add(record);
            if (simulateFailure) {
                callback.onCompletion(null, new RuntimeException("simulated"));
            } else {
                callback.onCompletion(null, null);
            }
        }
    }

    static final class FakeNxEvents implements NxEvents {
        @Override
        public void publishPremiumPurchase(PremiumPurchaseEvent event) {
        }

        @Override
        public void publishServerOnlineSnapshot(ServerOnlineSnapshotEvent event) {
        }

        @Override
        public void publishPrivateStoreSnapshot(PrivateStoreSnapshotEvent event) {
        }

        @Override
        public void publishPrivateStorePurchase(PrivateStorePurchaseEvent event) {
        }

        @Override
        public void publishCharacterPresence(CharacterPresenceEvent event) {
        }
    }

    private static final UUID OWN_SERVER_ID = UUID.fromString("019a0000-0000-7000-8000-000000000abc");

    private CommandTypeRegistry registry;
    private CapturingReplySender sender;
    private MockConsumer<byte[], byte[]> mockConsumer;

    @BeforeEach
    void setUp() {
        registry = new CommandTypeRegistry();
        sender = new CapturingReplySender();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    private CommandsConsumer build(String repliesTopic) {
        return new CommandsConsumer(
                "in",
                repliesTopic,
                OWN_SERVER_ID,
                new HostExecutorImpl(Runnable::run, 1000L),
                new FakeNxEvents(),
                Runnable::run,
                new app.l2nx.gs.adapter.core.sync.NxSyncImpl(),
                registry,
                mockConsumer,
                sender,
                new Gson(),
                CommandsConfig.defaults());
    }

    private static ConsumerRecord<byte[], byte[]> recordWithHeaders(String topic, String messageType,
                                                                    UUID corrId, byte[] value) {
        return recordWithHeaders(topic, messageType, corrId, value, OWN_SERVER_ID);
    }

    private static ConsumerRecord<byte[], byte[]> recordWithHeaders(String topic, String messageType,
                                                                    UUID corrId, byte[] value,
                                                                    UUID targetServerId) {
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>(topic, 0, 0L, new byte[]{1, 2}, value);
        if (messageType != null) {
            r.headers().add(NxHeaders.NX_MESSAGE_TYPE, messageType.getBytes(StandardCharsets.UTF_8));
        }
        if (corrId != null) {
            r.headers().add(NxHeaders.NX_CORRELATION_ID, corrId.toString().getBytes(StandardCharsets.UTF_8));
        }
        if (targetServerId != null) {
            r.headers().add(NxHeaders.NX_TARGET_SERVER_ID, NxHeaders.encodeUuid(targetServerId));
        }
        return r;
    }

    @Test
    void processRecord_happyPath_shouldInvokeHandlerAndSendSuccessReply() {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        CommandHandler<FakeCommand, Void> handler = (cmd, ctx) -> {
            handlerCalled.set(true);
            assertEquals(123L, cmd.getCharId().longValue());
            return CommandResult.ok();
        };
        registry.register(FakeCommand.class, handler);
        CommandsConsumer consumer = build("out");
        UUID corr = UUID.randomUUID();

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", corr,
                "{\"charId\":123}".getBytes(StandardCharsets.UTF_8)));

        assertTrue(handlerCalled.get());
        assertEquals(1L, consumer.handledTotal());
        assertEquals(1L, consumer.repliesPublishedTotal());
        assertEquals(1, sender.sent.size());

        ProducerRecord<byte[], Object> reply = sender.sent.get(0);
        assertEquals("out", reply.topic());
        assertArrayEquals("FakeResult".getBytes(StandardCharsets.UTF_8),
                reply.headers().lastHeader(NxHeaders.NX_MESSAGE_TYPE).value());
        assertArrayEquals(corr.toString().getBytes(StandardCharsets.UTF_8),
                reply.headers().lastHeader(NxHeaders.NX_CORRELATION_ID).value());
        CommandResult<?> body = (CommandResult<?>) reply.value();
        assertTrue(body.isOk());
    }

    @Test
    void processRecord_missingMessageTypeHeader_shouldReplyUnsupported() {
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", null, UUID.randomUUID(),
                "{}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.unsupportedTotal());
        assertEquals(1, sender.sent.size());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertFalse(body.isOk());
        assertEquals(CommandStatus.UNSUPPORTED_COMMAND, body.getStatus());
        assertEquals("missing-message-type-header",
                body.getProblem().getExtensions().get("error.cause"));
        assertArrayEquals("CommandResult".getBytes(StandardCharsets.UTF_8),
                sender.sent.get(0).headers().lastHeader(NxHeaders.NX_MESSAGE_TYPE).value());
    }

    @Test
    void processRecord_unknownType_shouldReplyUnsupported() {
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "GhostCommand", UUID.randomUUID(),
                "{}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.unsupportedTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(CommandStatus.UNSUPPORTED_COMMAND, body.getStatus());
        assertEquals("GhostCommand", body.getProblem().getExtensions().get("messageType"));
    }

    @Test
    void processRecord_badJson_shouldReplyValidationFailed() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.ok());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{not valid json".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.validationFailedTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(CommandStatus.VALIDATION_FAILED, body.getStatus());
        assertNotNull(body.getProblem().getDetail());
        assertNotNull(body.getProblem().getExtensions().get("error.class"));
    }

    @Test
    void processRecord_handlerThrowsRuntimeException_shouldReplyInternalError() {
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            throw new IllegalStateException("boom");
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.internalErrorsTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(CommandStatus.INTERNAL_ERROR, body.getStatus());
        assertEquals("IllegalStateException",
                body.getProblem().getExtensions().get("error.class"));
        assertEquals("boom", body.getProblem().getDetail());
    }

    @Test
    void processRecord_hostExecutorTimeout_shouldReplyUnavailable() {
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            throw new HostExecutorTimeoutException(30_000L);
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.internalErrorsTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(CommandStatus.UNAVAILABLE, body.getStatus());
        assertEquals("host-executor-timeout",
                body.getProblem().getExtensions().get("error.cause"));
        assertEquals(30_000L, body.getProblem().getExtensions().get("timeout.ms"));
    }

    @Test
    void processRecord_handlerReturnsNull_shouldReplyInternalError() {
        registry.register(FakeCommand.class, (cmd, ctx) -> null);
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.internalErrorsTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(CommandStatus.INTERNAL_ERROR, body.getStatus());
        assertEquals("handler-returned-null",
                body.getProblem().getExtensions().get("error.cause"));
    }

    @Test
    void processRecord_repliesTopicNull_shouldDropReplyAndIncrementFailed() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.ok());
        CommandsConsumer consumer = build(null);

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.handledTotal());
        assertEquals(0L, consumer.repliesPublishedTotal());
        assertEquals(1L, consumer.repliesFailedTotal());
        assertEquals(0, sender.sent.size(), "no record should be sent when repliesTopic is null");
    }

    @Test
    void processRecord_handlerCalledOnceWithDeserializedPayload() {
        AtomicInteger calls = new AtomicInteger();
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            calls.incrementAndGet();
            assertEquals(42L, cmd.getCharId().longValue());
            return CommandResult.ok();
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":42}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, calls.get());
    }

    @Test
    void processRecord_correlationIdMissing_shouldGenerateFallbackAndReply() {
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            assertNotNull(ctx.correlationId(), "fallback correlation id must be non-null");
            return CommandResult.ok();
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", null,
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, sender.sent.size());
    }

    @Test
    void processRecord_replySendCallbackFailure_shouldIncrementRepliesFailed() {
        sender.simulateFailure = true;
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.ok());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.repliesFailedTotal());
        assertEquals(0L, consumer.repliesPublishedTotal());
    }

    @Test
    void processRecord_shouldExposeIoExecutor_viaCtxIo() {
        AtomicBoolean ioObserved = new AtomicBoolean(false);
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            ctx.io().execute(() -> ioObserved.set(true));
            return CommandResult.ok();
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertTrue(ioObserved.get(),
                "ctx.io().execute() must run on the supplied executor (direct-run double)");
    }

    @Test
    void processRecord_pendingRepliesShouldDrainAfterCallback() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.ok());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, consumer.pendingReplies(),
                "callback fired synchronously in fake — pending should be back to 0");
    }

    @Test
    void processRecord_otherServerTarget_shouldSkipWithoutInvokingHandler() {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            handlerCalled.set(true);
            return CommandResult.ok();
        });
        CommandsConsumer consumer = build("out");
        UUID otherServer = UUID.fromString("019a0000-0000-7000-8000-00000000beef");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8), otherServer));

        assertFalse(handlerCalled.get(), "handler MUST NOT run for a record targeted at another server");
        assertEquals(1L, consumer.otherServerSkippedTotal());
        assertEquals(0, sender.sent.size(), "no reply must be sent for a foreign-target record");
    }

    @Test
    void processRecord_missingTargetServerHeader_shouldSkipWithoutInvokingHandler() {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        registry.register(FakeCommand.class, (cmd, ctx) -> {
            handlerCalled.set(true);
            return CommandResult.ok();
        });
        CommandsConsumer consumer = build("out");

        // null target → no NX_TARGET_SERVER_ID header stamped
        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8), null));

        assertFalse(handlerCalled.get(), "missing target header → strict drop");
        assertEquals(1L, consumer.otherServerSkippedTotal());
        assertEquals(0, sender.sent.size());
    }
}
