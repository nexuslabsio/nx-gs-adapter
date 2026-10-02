package app.l2nx.gs.adapter.core.commands;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.spi.capability.CommandHandler;
import app.l2nx.gs.adapter.api.spi.capability.DeferredReply;
import app.l2nx.gs.adapter.core.commands.CommandsConsumerTest.CapturingReplySender;
import app.l2nx.gs.adapter.core.commands.CommandsConsumerTest.FakeCommand;
import app.l2nx.gs.adapter.core.commands.CommandsConsumerTest.FakeNxEvents;
import app.l2nx.gs.adapter.core.kafka.gson.AdapterGson;
import app.l2nx.gs.adapter.core.sync.NxSyncImpl;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeferredReplyTest {

    private static final UUID OWN_SERVER_ID = UUID.fromString("019a0000-0000-7000-8000-000000000abc");

    private CommandTypeRegistry registry;
    private CapturingReplySender sender;
    private DeferredReplies deferredReplies;
    private AtomicReference<DeferredReply<Void>> taken;

    @BeforeEach
    void setUp() {
        registry = new CommandTypeRegistry();
        sender = new CapturingReplySender();
        taken = new AtomicReference<>();
    }

    private CommandsConsumer consumer(long maxMs) {
        deferredReplies = new DeferredReplies(maxMs);
        return new CommandsConsumer(
                "in",
                "out",
                OWN_SERVER_ID,
                new HostExecutorImpl(Runnable::run, 1000L),
                new FakeNxEvents(),
                Runnable::run,
                new NxSyncImpl(),
                registry,
                deferredReplies,
                new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST),
                sender,
                AdapterGson.create(),
                CommandsConfig.defaults());
    }

    private void register(CommandHandler<FakeCommand, Void> handler) {
        registry.register(FakeCommand.class, handler);
    }

    private static ConsumerRecord<byte[], byte[]> record(UUID corr) {
        ConsumerRecord<byte[], byte[]> r =
                new ConsumerRecord<>("in", 0, 0L, new byte[] {1}, "{\"charId\":1}".getBytes(StandardCharsets.UTF_8));
        r.headers().add(NxHeaders.NX_MESSAGE_TYPE, "FakeCommand".getBytes(StandardCharsets.UTF_8));
        r.headers().add(NxHeaders.NX_CORRELATION_ID, corr.toString().getBytes(StandardCharsets.UTF_8));
        r.headers().add(NxHeaders.NX_TARGET_SERVER_ID, NxHeaders.encodeUuid(OWN_SERVER_ID));
        return r;
    }

    private CommandResult<?> onlyReply() {
        assertEquals(1, sender.sent.size());
        return (CommandResult<?>) sender.sent.get(0).value();
    }

    private void deferringHandler() {
        register((cmd, ctx) -> {
            DeferredReply<Void> reply = ctx.deferReply();
            taken.set(reply);
            return reply.pending();
        });
    }

    @Nested
    class Pending {

        @Test
        void processRecord_shouldNotReply_whenHandlerReturnsPending() {
            deferringHandler();
            CommandsConsumer consumer = consumer(60_000L);

            consumer.processRecord(record(UUID.randomUUID()));

            assertTrue(sender.sent.isEmpty());
            assertEquals(1L, consumer.handledTotal());
            assertEquals(1L, deferredReplies.openCount());
        }

        @Test
        void complete_shouldPublishUnderOriginalCorrelationAndType() {
            deferringHandler();
            CommandsConsumer consumer = consumer(60_000L);
            UUID corr = UUID.randomUUID();
            consumer.processRecord(record(corr));

            assertTrue(taken.get().complete(CommandResult.<Void>ok()));

            ProducerRecord<byte[], Object> reply = sender.sent.get(0);
            assertArrayEquals(
                    corr.toString().getBytes(StandardCharsets.UTF_8),
                    reply.headers().lastHeader(NxHeaders.NX_CORRELATION_ID).value());
            assertArrayEquals(
                    "FakeResult".getBytes(StandardCharsets.UTF_8),
                    reply.headers().lastHeader(NxHeaders.NX_MESSAGE_TYPE).value());
            assertTrue(onlyReply().isOk());
            assertEquals(0L, deferredReplies.openCount());
        }

        @Test
        void complete_shouldReturnFalseAndNotPublish_whenAlreadyCompleted() {
            deferringHandler();
            consumer(60_000L).processRecord(record(UUID.randomUUID()));
            taken.get().complete(CommandResult.<Void>ok());

            assertFalse(taken.get().complete(CommandResult.<Void>ok()));
            assertEquals(1, sender.sent.size());
        }

        @Test
        void complete_shouldPublishInternalError_whenResultIsNull() {
            deferringHandler();
            consumer(60_000L).processRecord(record(UUID.randomUUID()));

            taken.get().complete(null);

            assertEquals(CommandStatus.INTERNAL_ERROR, onlyReply().getStatus());
            assertEquals(
                    "deferred-null-result",
                    onlyReply().getProblem().getExtensions().get("error.cause"));
        }

        @Test
        void deferReply_shouldReturnSameHandle_whenCalledTwice() {
            AtomicReference<DeferredReply<Void>> second = new AtomicReference<>();
            register((cmd, ctx) -> {
                DeferredReply<Void> first = ctx.deferReply();
                second.set(ctx.deferReply());
                taken.set(first);
                return first.pending();
            });

            consumer(60_000L).processRecord(record(UUID.randomUUID()));

            assertSame(taken.get(), second.get());
        }
    }

    @Nested
    class Expiry {

        @Test
        void expire_shouldPublishInternalError_whenHostNeverCompletes() throws InterruptedException {
            deferringHandler();
            CommandsConsumer consumer = consumer(50L);
            consumer.processRecord(record(UUID.randomUUID()));

            long deadline = System.currentTimeMillis() + 5_000L;
            while (sender.sent.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }

            assertEquals(CommandStatus.INTERNAL_ERROR, onlyReply().getStatus());
            assertEquals(
                    "deferred-reply-expired",
                    onlyReply().getProblem().getExtensions().get("error.cause"));
            assertEquals(1L, deferredReplies.expiredTotal());
            assertFalse(taken.get().complete(CommandResult.<Void>ok()));
            assertEquals(1L, consumer.currentStats().getDeferredExpiredTotal());
        }
    }

    @Nested
    class DirectAnswer {

        @Test
        void processRecord_shouldPublishAndCloseHandle_whenHandlerTakesHandleButReturnsResult() {
            register((cmd, ctx) -> {
                taken.set(ctx.<Void>deferReply());
                return CommandResult.invalidState("already running");
            });

            consumer(60_000L).processRecord(record(UUID.randomUUID()));

            assertEquals(CommandStatus.INVALID_STATE, onlyReply().getStatus());
            assertFalse(taken.get().complete(CommandResult.<Void>ok()));
            assertEquals(0L, deferredReplies.openCount());
        }

        @Test
        void processRecord_shouldPublishInternalError_whenHandlerReturnsForeignMarker() {
            DeferredReplies other = new DeferredReplies(60_000L);
            DeferredReplyImpl<Void> foreign = other.open(UUID.randomUUID(), new byte[0], (c, t, r) -> {});
            register((cmd, ctx) -> foreign.pending());

            consumer(60_000L).processRecord(record(UUID.randomUUID()));

            assertEquals(CommandStatus.INTERNAL_ERROR, onlyReply().getStatus());
            assertEquals(
                    "foreign-deferred-marker",
                    onlyReply().getProblem().getExtensions().get("error.cause"));
            other.shutdown();
        }
    }

    @Nested
    class Shutdown {

        @Test
        void shutdown_shouldCloseOpenHandlesWithUnavailable() {
            deferringHandler();
            consumer(60_000L).processRecord(record(UUID.randomUUID()));

            deferredReplies.shutdown();

            assertEquals(CommandStatus.UNAVAILABLE, onlyReply().getStatus());
            assertEquals(
                    "host-shutdown", onlyReply().getProblem().getExtensions().get("error.cause"));
            assertFalse(taken.get().complete(CommandResult.<Void>ok()));
        }

        @Test
        void currentStats_shouldReportOpenDeferredReplies() {
            deferringHandler();
            CommandsConsumer consumer = consumer(60_000L);
            consumer.processRecord(record(UUID.randomUUID()));

            assertEquals(1L, consumer.currentStats().getDeferredOpen());
        }
    }
}
