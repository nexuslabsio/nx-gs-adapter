package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.ErrorCode;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreEvent;
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
        public void publishServerOnline(ServerOnlineSnapshotEvent event) {
        }

        @Override
        public void publishPrivateStore(PrivateStoreEvent event) {
        }
    }

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
                new HostExecutorImpl(Runnable::run, 1000L),
                new FakeNxEvents(),
                registry,
                mockConsumer,
                sender,
                new Gson(),
                CommandsConfig.defaults());
    }

    private static ConsumerRecord<byte[], byte[]> recordWithHeaders(String topic, String messageType,
                                                                    UUID corrId, byte[] value) {
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>(topic, 0, 0L, new byte[]{1, 2}, value);
        if (messageType != null) {
            r.headers().add(NxHeaders.NX_MESSAGE_TYPE, messageType.getBytes(StandardCharsets.UTF_8));
        }
        if (corrId != null) {
            r.headers().add(NxHeaders.NX_CORRELATION_ID, corrId.toString().getBytes(StandardCharsets.UTF_8));
        }
        return r;
    }

    @Test
    void processRecord_happyPath_shouldInvokeHandlerAndSendSuccessReply() {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        CommandHandler<FakeCommand, Void> handler = (cmd, ctx) -> {
            handlerCalled.set(true);
            assertEquals(123L, cmd.getCharId().longValue());
            return CommandResult.success();
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
        assertArrayEquals("FakeCommandResult".getBytes(StandardCharsets.UTF_8),
                reply.headers().lastHeader(NxHeaders.NX_MESSAGE_TYPE).value());
        assertArrayEquals(corr.toString().getBytes(StandardCharsets.UTF_8),
                reply.headers().lastHeader(NxHeaders.NX_CORRELATION_ID).value());
        CommandResult<?> body = (CommandResult<?>) reply.value();
        assertTrue(body.isSuccess());
    }

    @Test
    void processRecord_missingMessageTypeHeader_shouldReplyUnsupported() {
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", null, UUID.randomUUID(),
                "{}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.unsupportedTotal());
        assertEquals(1, sender.sent.size());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertFalse(body.isSuccess());
        assertEquals(ErrorCode.UNSUPPORTED_COMMAND, body.getErrorCode());
        assertEquals("missing-message-type-header", body.getErrorDetails().get("error.cause"));
        // I5: fallback header should be "CommandResult", not "UnknownResult"
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
        assertEquals(ErrorCode.UNSUPPORTED_COMMAND, body.getErrorCode());
        assertEquals("unregistered-type", body.getErrorDetails().get("error.cause"));
    }

    @Test
    void processRecord_badJson_shouldReplyValidationFailed() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.success());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{not valid json".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.validationFailedTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(ErrorCode.VALIDATION_FAILED, body.getErrorCode());
        assertNotNull(body.getErrorDetails().get("parse"));
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
        assertEquals(ErrorCode.INTERNAL_ERROR, body.getErrorCode());
        assertEquals("IllegalStateException", body.getErrorDetails().get("error.class"));
        assertEquals("boom", body.getErrorDetails().get("error.message"));
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
        assertEquals(ErrorCode.UNAVAILABLE, body.getErrorCode());
        assertEquals("host-executor-timeout", body.getErrorDetails().get("error.cause"));
        assertEquals("30000", body.getErrorDetails().get("timeout.ms"));
    }

    @Test
    void processRecord_handlerReturnsNull_shouldReplyInternalError() {
        registry.register(FakeCommand.class, (cmd, ctx) -> null);
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.internalErrorsTotal());
        CommandResult<?> body = (CommandResult<?>) sender.sent.get(0).value();
        assertEquals(ErrorCode.INTERNAL_ERROR, body.getErrorCode());
        assertEquals("handler-returned-null", body.getErrorDetails().get("error.cause"));
    }

    @Test
    void processRecord_repliesTopicNull_shouldDropReplyAndIncrementFailed() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.success());
        CommandsConsumer consumer = build(null); // I1 — no replies topic configured

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
            return CommandResult.success();
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
            return CommandResult.success();
        });
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", null,
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, sender.sent.size());
    }

    @Test
    void processRecord_replySendCallbackFailure_shouldIncrementRepliesFailed() {
        sender.simulateFailure = true;
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.success());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1L, consumer.repliesFailedTotal());
        assertEquals(0L, consumer.repliesPublishedTotal());
    }

    @Test
    void processRecord_pendingRepliesShouldDrainAfterCallback() {
        registry.register(FakeCommand.class, (cmd, ctx) -> CommandResult.success());
        CommandsConsumer consumer = build("out");

        consumer.processRecord(recordWithHeaders("in", "FakeCommand", UUID.randomUUID(),
                "{\"charId\":1}".getBytes(StandardCharsets.UTF_8)));

        assertEquals(0, consumer.pendingReplies(),
                "callback fired synchronously in fake — pending should be back to 0");
    }
}
