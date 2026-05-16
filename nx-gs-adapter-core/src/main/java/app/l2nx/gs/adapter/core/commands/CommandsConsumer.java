package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.NxHeaders;
import app.l2nx.gs.adapter.api.kafka.commands.CommandProblem;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.kafka.ops.CommandsStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.spi.*;
import app.l2nx.gs.commons.UUIDv7;
import app.l2nx.gs.commons.bytes.LongBytes;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Inbound commands consumer + dispatcher. Single Kafka consumer on the
 * {@code nx-commands-consumer} daemon thread; {@link CommandHandler#handle}
 * runs synchronously on it, so game-state mutations MUST hop via
 * {@link HostExecutor#sync(Runnable)} (bounded by
 * {@code l2nx.commands.host-sync-timeout-ms}).
 *
 * <p><b>At-most-once.</b> {@code commitSync} runs BEFORE dispatch — a
 * crash or commit failure mid-batch drops the in-flight records (no
 * redelivery). Caller times out, operator re-issues. Handlers do NOT
 * need to be idempotent. Reply sends are fire-and-forget.</p>
 *
 * <p>Error boundaries: unknown {@code Nx-Message-Type} →
 * {@link CommandStatus#UNSUPPORTED_COMMAND}; Gson failure →
 * {@link CommandStatus#VALIDATION_FAILED}; {@link HostExecutorTimeoutException}
 * → {@link CommandStatus#UNAVAILABLE}; other {@code RuntimeException} or
 * {@code null} return → {@link CommandStatus#INTERNAL_ERROR}; {@code Error}
 * (OOM) escapes uncaught.</p>
 */
public final class CommandsConsumer {

    private static final NxLog log = NxLogFactory.getLogger(CommandsConsumer.class);

    private static final long SHUTDOWN_GRACE_MS = 1_000L;
    private static final byte[] FALLBACK_REPLY_TYPE_BYTES =
            "CommandResult".getBytes(StandardCharsets.UTF_8);

    /**
     * Bridge to the actual Kafka send. Production wires this to
     * {@code (record, callback) -> NxKafka.instance().sendBytesKeyRecord(record, callback)}.
     */
    @FunctionalInterface
    public interface ReplySender {
        void send(ProducerRecord<byte[], Object> record, org.apache.kafka.clients.producer.Callback callback);
    }

    private final String inboundTopic;
    private final @Nullable String repliesTopic;
    private final HostExecutor hostExecutor;
    private final NxEvents events;
    private final Executor ioExecutor;
    private final NxSync sync;
    private final CommandTypeRegistry registry;
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final ReplySender replySender;
    private final Gson gson;
    private final long pollTimeoutMs;
    private final long shutdownTimeoutMs;
    private final long replyFlushTimeoutMs;
    private final Thread daemon;

    private final AtomicLong consumedTotal = new AtomicLong();
    private final AtomicLong handledTotal = new AtomicLong();
    private final AtomicLong unsupportedTotal = new AtomicLong();
    private final AtomicLong validationFailedTotal = new AtomicLong();
    private final AtomicLong internalErrorsTotal = new AtomicLong();
    private final AtomicLong repliesPublishedTotal = new AtomicLong();
    private final AtomicLong repliesFailedTotal = new AtomicLong();
    private final AtomicLong commitFailuresTotal = new AtomicLong();
    private final AtomicLong replyFlushTimeoutsTotal = new AtomicLong();

    private final AtomicInteger pendingReplies = new AtomicInteger(0);
    private final Object replyDrainLock = new Object();

    private volatile boolean running = false;

    CommandsConsumer(String inboundTopic,
                     @Nullable String repliesTopic,
                     HostExecutor hostExecutor,
                     NxEvents events,
                     Executor ioExecutor,
                     NxSync sync,
                     CommandTypeRegistry registry,
                     Consumer<byte[], byte[]> kafkaConsumer,
                     ReplySender replySender,
                     Gson gson,
                     CommandsConfig config) {
        this.inboundTopic = inboundTopic;
        this.repliesTopic = repliesTopic;
        this.hostExecutor = hostExecutor;
        this.events = events;
        this.ioExecutor = ioExecutor;
        this.sync = sync;
        this.registry = registry;
        this.kafkaConsumer = kafkaConsumer;
        this.replySender = replySender;
        this.gson = gson;
        this.pollTimeoutMs = Math.max(1L, config.getPollTimeoutMs());
        this.shutdownTimeoutMs = Math.max(0L, config.getShutdownTimeoutMs());
        this.replyFlushTimeoutMs = Math.max(0L, config.getReplyFlushTimeoutMs());
        this.daemon = new Thread(SafeRunnable.wrap(this::pollLoop, log), "nx-commands-consumer");
        this.daemon.setDaemon(true);
    }

    /**
     * Spawn the consumer daemon and subscribe to {@code inboundTopic}.
     * Idempotent. Package-private — callers go through
     * {@link CommandsBootstrap}.
     */
    void start() {
        if (running) {
            return;
        }
        running = true;
        kafkaConsumer.subscribe(Collections.singletonList(inboundTopic));
        daemon.start();
        log.info("Commands consumer started — topic={}, replies={}", inboundTopic, repliesTopic);
    }

    /**
     * Signal the daemon to stop, wake it from any blocking poll, await join
     * up to {@code shutdownTimeoutMs}, then close the Kafka consumer.
     * Idempotent.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        try {
            kafkaConsumer.wakeup();
        } catch (Throwable t) {
            log.warn("Commands consumer wakeup threw {}: {}",
                    t.getClass().getName(), t.getMessage(), t);
        }
        try {
            daemon.join(shutdownTimeoutMs + SHUTDOWN_GRACE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            kafkaConsumer.close(Duration.ofMillis(SHUTDOWN_GRACE_MS));
        } catch (Throwable t) {
            log.warn("Commands consumer close threw {}: {}",
                    t.getClass().getName(), t.getMessage(), t);
        }
        log.info("Commands consumer stopped");
    }

    /**
     * Heartbeat slot snapshot for the {@code commands} module.
     */
    public ModuleStatus currentStatus() {
        CommandsStats stats = currentStats();
        return ModuleStatus.builder()
                .name("commands")
                .state(running ? "ACTIVE" : "DISABLED")
                .stats(ModuleStatus.Stats.builder().commands(stats).build())
                .build();
    }

    CommandsStats currentStats() {
        return CommandsStats.builder()
                .consumedTotal(consumedTotal.get())
                .handledTotal(handledTotal.get())
                .unsupportedTotal(unsupportedTotal.get())
                .validationFailedTotal(validationFailedTotal.get())
                .internalErrorsTotal(internalErrorsTotal.get())
                .repliesPublishedTotal(repliesPublishedTotal.get())
                .repliesFailedTotal(repliesFailedTotal.get())
                .commitFailuresTotal(commitFailuresTotal.get())
                .registeredTypes(registry.snapshotRegisteredTypes())
                .build();
    }

    private void pollLoop() {
        while (running) {
            ConsumerRecords<byte[], byte[]> records;
            try {
                records = kafkaConsumer.poll(Duration.ofMillis(pollTimeoutMs));
            } catch (WakeupException wakeup) {
                if (!running) {
                    return; // expected shutdown path
                }
                log.warn("Unexpected wakeup on commands consumer");
                continue;
            } catch (Throwable t) {
                if (!running) {
                    return;
                }
                log.error("Commands consumer poll error: {} — backing off 1s", t.getClass().getName(), t);
                sleepQuiet(1_000L);
                continue;
            }

            if (records.isEmpty()) {
                continue;
            }

            try {
                kafkaConsumer.commitSync();
            } catch (Throwable t) {
                commitFailuresTotal.incrementAndGet();
                log.warn("Commands consumer commit failed: {} ({}) — dropping batch (at-most-once)",
                        t.getClass().getName(), t.getMessage(), t);
                continue;
            }

            for (ConsumerRecord<byte[], byte[]> record : records) {
                consumedTotal.incrementAndGet();
                processRecord(record);
            }
        }
    }

    /**
     * Block the consumer thread until every reply send started during the
     * current batch has fired its callback (or the configured flush timeout
     * elapses). Bounds the at-most-once reply window: with this gate in
     * place, a JVM crash before the gate clears leaves the batch
     * uncommitted, so records redeliver and replies are re-emitted by the
     * idempotent handlers.
     *
     * <p>Skipped when {@code replyFlushTimeoutMs == 0} — interpreted as
     * "operator opts out of flushing"; the at-most-once window reopens.</p>
     *
     * @return {@code true} when the wait returned via thread interrupt with
     * pending replies still outstanding — caller MUST skip the offset commit
     * so the batch redelivers. {@code false} otherwise (drained or timed
     * out cleanly).
     */
    private boolean awaitRepliesDrain() {
        if (replyFlushTimeoutMs <= 0L) {
            return false;
        }
        long deadline = System.currentTimeMillis() + replyFlushTimeoutMs;
        synchronized (replyDrainLock) {
            while (pendingReplies.get() > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0L) {
                    int leftover = pendingReplies.get();
                    if (leftover > 0) {
                        replyFlushTimeoutsTotal.incrementAndGet();
                        log.warn("Reply flush timed out with {} send(s) still in flight after {}ms — "
                                + "committing offset anyway; pending sends will continue and may still "
                                + "succeed asynchronously", leftover, replyFlushTimeoutMs);
                    }
                    return false;
                }
                try {
                    replyDrainLock.wait(remaining);
                } catch (InterruptedException ie) {
                    int leftover = pendingReplies.get();
                    running = false;
                    Thread.currentThread().interrupt();
                    if (leftover > 0) {
                        log.warn("Interrupted during reply drain; skipping offset commit so {} "
                                + "pending record(s) will redeliver", leftover);
                        return true;
                    }
                    return false;
                }
            }
        }
        return false;
    }

    // package-visible for unit tests; production callers go through pollLoop()
    void processRecord(ConsumerRecord<byte[], byte[]> record) {
        Headers headers = record.headers();
        String messageType = readStringHeader(headers, NxHeaders.NX_MESSAGE_TYPE);
        UUID correlationId = readCorrelationId(headers);

        // 1. Resolve binding
        if (messageType == null || messageType.isEmpty()) {
            unsupportedTotal.incrementAndGet();
            log.warn("Inbound command record missing {} header — replying UNSUPPORTED_COMMAND",
                    NxHeaders.NX_MESSAGE_TYPE);
            sendReply(correlationId, FALLBACK_REPLY_TYPE_BYTES,
                    CommandResult.error(CommandStatus.UNSUPPORTED_COMMAND,
                            "Inbound command missing Nx-Message-Type header",
                            "error.cause", "missing-message-type-header"));
            return;
        }
        CommandTypeBinding binding = registry.lookup(messageType);
        if (binding == null) {
            unsupportedTotal.incrementAndGet();
            log.warn("No registered handler for command type {} — replying UNSUPPORTED_COMMAND",
                    messageType);
            sendReply(correlationId, computeReplyTypeBytes(messageType),
                    CommandResult.error(CommandStatus.UNSUPPORTED_COMMAND,
                            "No registered handler for command type",
                            "messageType", messageType));
            return;
        }

        byte[] replyTypeBytes = binding.replyMessageTypeBytes();

        // 2. Deserialize
        NxCommand<?> command;
        try {
            byte[] value = record.value();
            String json = (value == null) ? "{}" : new String(value, StandardCharsets.UTF_8);
            command = gson.fromJson(json, binding.commandClass());
            if (command == null) {
                throw new JsonSyntaxException("Gson returned null for non-null payload");
            }
        } catch (JsonSyntaxException jse) {
            validationFailedTotal.incrementAndGet();
            log.warn("Failed to deserialize command type {} (corr={}): {}",
                    messageType, correlationId, jse.getMessage());
            sendReply(correlationId, replyTypeBytes,
                    CommandResult.error(CommandStatus.VALIDATION_FAILED,
                            CommandProblem.builder()
                                    .title("Failed to deserialize command payload")
                                    .detail(jse.getMessage())
                                    .extension("error.class", jse.getClass().getSimpleName())
                                    .build()));
            return;
        } catch (Throwable t) {
            internalErrorsTotal.incrementAndGet();
            log.error("Unexpected deserialization failure for command type {} (corr={}): {}",
                    messageType, correlationId, t.getClass().getName(), t);
            sendReply(correlationId, replyTypeBytes,
                    CommandResult.error(CommandStatus.INTERNAL_ERROR,
                            CommandProblem.builder()
                                    .title("Deserialization failure")
                                    .detail(String.valueOf(t.getMessage()))
                                    .extension("error.class", t.getClass().getSimpleName())
                                    .build()));
            return;
        }

        // 3. Invoke handler — Throwable (Error) intentionally NOT caught (lets JVM-level handler take over)
        CommandContext ctx = new CommandContextImpl(correlationId, hostExecutor, events, ioExecutor, sync);
        @SuppressWarnings({"unchecked", "rawtypes"})
        CommandHandler handler = binding.handler();
        CommandResult<?> result;
        try {
            @SuppressWarnings("unchecked")
            CommandResult<?> raw = handler.handle(command, ctx);
            result = raw;
        } catch (HostExecutorTimeoutException hte) {
            internalErrorsTotal.incrementAndGet();
            log.warn("Handler for {} (corr={}) hit host-executor timeout after {}ms",
                    messageType, correlationId, hte.getTimeoutMs());
            result = CommandResult.error(CommandStatus.UNAVAILABLE,
                    CommandProblem.builder()
                            .title("Host executor timeout")
                            .extension("error.cause", "host-executor-timeout")
                            .extension("timeout.ms", hte.getTimeoutMs())
                            .build());
        } catch (RuntimeException re) {
            internalErrorsTotal.incrementAndGet();
            log.warn("Handler for {} (corr={}) threw {}: {}",
                    messageType, correlationId, re.getClass().getSimpleName(), re.getMessage(), re);
            result = CommandResult.error(CommandStatus.INTERNAL_ERROR,
                    CommandProblem.builder()
                            .title("Handler threw " + re.getClass().getSimpleName())
                            .detail(String.valueOf(re.getMessage()))
                            .extension("error.class", re.getClass().getSimpleName())
                            .build());
        }

        // 4. Reply
        if (result == null) {
            internalErrorsTotal.incrementAndGet();
            log.warn("Handler for {} (corr={}) returned null — auto-wrapping as INTERNAL_ERROR",
                    messageType, correlationId);
            result = CommandResult.error(CommandStatus.INTERNAL_ERROR,
                    "Handler returned null",
                    "error.cause", "handler-returned-null");
        } else {
            handledTotal.incrementAndGet();
        }
        sendReply(correlationId, replyTypeBytes, result);
    }

    private void sendReply(UUID correlationId, byte[] replyMessageTypeBytes, CommandResult<?> result) {
        if (repliesTopic == null) {
            // Spec edge case: commandsTopic configured but commandsRepliesTopic absent.
            // Increment repliesFailedTotal so the heartbeat surfaces "100% reply loss"
            // — operators reading {consumed > 0, replies-published == 0} would otherwise
            // see no failure signal.
            repliesFailedTotal.incrementAndGet();
            log.debug("Reply for corr={} dropped — repliesTopic not configured", correlationId);
            return;
        }
        byte[] key = LongBytes.bigEndian(correlationId.getMostSignificantBits());
        ProducerRecord<byte[], Object> record = new ProducerRecord<byte[], Object>(
                repliesTopic, key, result);
        record.headers().add(NxHeaders.NX_CORRELATION_ID,
                correlationId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(NxHeaders.NX_MESSAGE_TYPE, replyMessageTypeBytes);

        pendingReplies.incrementAndGet();
        try {
            replySender.send(record, (metadata, exception) -> {
                try {
                    if (exception != null) {
                        repliesFailedTotal.incrementAndGet();
                        log.warn("Reply send failed for corr={}: {}", correlationId, exception.getMessage(), exception);
                    } else {
                        repliesPublishedTotal.incrementAndGet();
                    }
                } finally {
                    if (pendingReplies.decrementAndGet() == 0) {
                        synchronized (replyDrainLock) {
                            replyDrainLock.notifyAll();
                        }
                    }
                }
            });
        } catch (Throwable t) {
            // send() itself threw synchronously (rare — usually invalid config or producer-closed).
            // Decrement the counter we incremented above so awaitRepliesDrain doesn't hang waiting
            // for a callback that will never fire.
            repliesFailedTotal.incrementAndGet();
            if (pendingReplies.decrementAndGet() == 0) {
                synchronized (replyDrainLock) {
                    replyDrainLock.notifyAll();
                }
            }
            log.error("Reply send threw for corr={}: {} ({})",
                    correlationId, t.getClass().getName(), t.getMessage(), t);
        }
    }

    private static byte[] computeReplyTypeBytes(String originalMessageType) {
        return CommandTypeBinding.deriveReplyTypeName(originalMessageType)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static @Nullable String readStringHeader(Headers headers, String name) {
        if (headers == null) {
            return null;
        }
        Header h = headers.lastHeader(name);
        if (h == null || h.value() == null) {
            return null;
        }
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    /**
     * Read {@link NxHeaders#NX_CORRELATION_ID}. Tolerates missing / malformed
     * values by generating a fallback UUIDv7 — the handler still gets a
     * stable id for log tagging, and the WARN log surfaces the misconfiguration.
     */
    private UUID readCorrelationId(Headers headers) {
        String raw = readStringHeader(headers, NxHeaders.NX_CORRELATION_ID);
        if (raw == null || raw.isEmpty()) {
            UUID fallback = UUIDv7.generate();
            log.warn("Inbound command record missing {} header — generated fallback {}",
                    NxHeaders.NX_CORRELATION_ID, fallback);
            return fallback;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            UUID fallback = UUIDv7.generate();
            log.warn("Inbound command record has malformed {} header '{}' — generated fallback {}",
                    NxHeaders.NX_CORRELATION_ID, raw, fallback);
            return fallback;
        }
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // Test seams — visible only to package.

    long consumedTotal() {
        return consumedTotal.get();
    }

    long handledTotal() {
        return handledTotal.get();
    }

    long unsupportedTotal() {
        return unsupportedTotal.get();
    }

    long validationFailedTotal() {
        return validationFailedTotal.get();
    }

    long internalErrorsTotal() {
        return internalErrorsTotal.get();
    }

    long repliesPublishedTotal() {
        return repliesPublishedTotal.get();
    }

    long repliesFailedTotal() {
        return repliesFailedTotal.get();
    }

    long commitFailuresTotal() {
        return commitFailuresTotal.get();
    }

    long replyFlushTimeoutsTotal() {
        return replyFlushTimeoutsTotal.get();
    }

    int pendingReplies() {
        return pendingReplies.get();
    }
}
