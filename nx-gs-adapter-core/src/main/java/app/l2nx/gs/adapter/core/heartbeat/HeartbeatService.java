package app.l2nx.gs.adapter.core.heartbeat;

import app.l2nx.gs.adapter.api.kafka.HeartbeatEvent;
import app.l2nx.gs.adapter.core.concurrent.SafeRunnable;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Publishes a {@link HeartbeatEvent} every {@value #PERIOD_SECONDS} seconds via the
 * supplied {@link KafkaPublisher} on the supplied scheduler. {@code start()} captures
 * {@code connectInstant} fresh each invocation so {@code uptime} is session-scoped.
 */
public final class HeartbeatService {

    private static final NxLog log = NxLogFactory.getLogger(HeartbeatService.class);

    static final long PERIOD_SECONDS = 60L;

    @FunctionalInterface
    public interface KafkaPublisher {
        void send(String topic, String key, Object payload);
    }

    private final KafkaPublisher publisher;
    private final ScheduledExecutorService scheduler;
    private final String adapterVersion;
    private final Supplier<Instant> clock;
    private final AtomicReference<Session> session = new AtomicReference<Session>();

    public HeartbeatService(KafkaPublisher publisher,
                            ScheduledExecutorService scheduler,
                            String adapterVersion) {
        this(publisher, scheduler, adapterVersion, () -> Instant.now());
    }

    HeartbeatService(KafkaPublisher publisher,
                     ScheduledExecutorService scheduler,
                     String adapterVersion,
                     Supplier<Instant> clock) {
        this.publisher = publisher;
        this.scheduler = scheduler;
        this.adapterVersion = adapterVersion;
        this.clock = clock;
    }

    public void start(String serverId, String heartbeatTopic) {
        Session previous = session.getAndSet(null);
        if (previous != null) {
            previous.future.cancel(false);
        }
        final Instant connectInstant = clock.get();
        // Wrapped on top of tick()'s own try/catch — an uncaught exception inside a
        // ScheduledExecutorService task cancels future invocations of that task.
        Runnable tick = SafeRunnable.wrap(() -> tick(serverId, heartbeatTopic, connectInstant), log);
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
                tick, PERIOD_SECONDS, PERIOD_SECONDS, TimeUnit.SECONDS);
        session.set(new Session(connectInstant, future));
    }

    public void stop() {
        Session current = session.getAndSet(null);
        if (current != null) {
            current.future.cancel(false);
        }
    }

    private void tick(String serverId, String heartbeatTopic, Instant connectInstant) {
        try {
            long uptime = ChronoUnit.SECONDS.between(connectInstant, clock.get());
            HeartbeatEvent event = new HeartbeatEvent(serverId, adapterVersion, uptime);
            publisher.send(heartbeatTopic, serverId, event);
        } catch (Throwable t) {
            log.error("Heartbeat tick failed: {}", t.getClass().getName());
        }
    }

    private static final class Session {
        final Instant connectInstant;
        final ScheduledFuture<?> future;

        Session(Instant connectInstant, ScheduledFuture<?> future) {
            this.connectInstant = connectInstant;
            this.future = future;
        }
    }
}
