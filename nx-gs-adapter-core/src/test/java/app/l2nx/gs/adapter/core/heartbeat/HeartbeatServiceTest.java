package app.l2nx.gs.adapter.core.heartbeat;

import app.l2nx.gs.adapter.api.kafka.HeartbeatEvent;
import app.l2nx.gs.adapter.core.concurrent.CapturingScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatServiceTest {

    private CapturingScheduler scheduler;
    private RecordingPublisher publisher;
    private AtomicReference<Instant> now;

    @BeforeEach
    void setUp() {
        scheduler = new CapturingScheduler();
        publisher = new RecordingPublisher();
        now = new AtomicReference<Instant>(Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void start_shouldScheduleEvery60Seconds() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", clock());

        service.start("server-uuid", "nexus.heartbeat");

        assertNotNull(scheduler.fixedDelayRunnable);
        assertEquals(60L, scheduler.fixedInitialDelay);
        assertEquals(60L, scheduler.fixedPeriod);
        assertEquals(TimeUnit.SECONDS, scheduler.fixedUnit);
    }

    @Test
    void start_shouldPublishPayload_onTick() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", clock());
        service.start("server-uuid", "nexus.heartbeat");

        now.set(now.get().plusSeconds(60));
        scheduler.runFixedDelayOnce();

        assertEquals(1, publisher.calls.size());
        RecordingPublisher.Call call = publisher.calls.get(0);
        assertEquals("nexus.heartbeat", call.topic);
        assertEquals("server-uuid", call.key);
        HeartbeatEvent event = (HeartbeatEvent) call.payload;
        assertEquals("server-uuid", event.getServerId());
        assertEquals("1.2.3", event.getAdapterVersion());
        assertEquals(60L, event.getUptime());
    }

    @Test
    void start_shouldResetUptime_whenCalledTwice() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", clock());

        service.start("server-uuid", "nexus.heartbeat");
        CapturingScheduler.CancellableFuture firstFuture = scheduler.fixedFuture;
        now.set(now.get().plusSeconds(300));

        service.start("server-uuid", "nexus.heartbeat");
        assertTrue(firstFuture.cancelled);

        now.set(now.get().plusSeconds(30));
        scheduler.runFixedDelayOnce();

        HeartbeatEvent event = (HeartbeatEvent) publisher.calls.get(0).payload;
        // 30, not 330 — connectInstant was recaptured at the second start().
        assertEquals(30L, event.getUptime());
    }

    @Test
    void stop_shouldCancelInFlightSchedule() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", clock());
        service.start("server-uuid", "nexus.heartbeat");

        service.stop();

        assertTrue(scheduler.fixedFuture.cancelled);
    }

    @Test
    void stop_shouldBeIdempotent_whenNeverStarted() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", clock());

        service.stop();
        service.stop();
    }

    @Test
    void tick_shouldNotPropagate_whenPublisherThrows() {
        ThrowingPublisher throwing = new ThrowingPublisher();
        HeartbeatService service = new HeartbeatService(throwing, scheduler, "1.2.3", clock());

        service.start("server-uuid", "nexus.heartbeat");
        scheduler.runFixedDelayOnce();

        // A propagated tick would cancel the scheduled task.
        assertEquals(1, throwing.invocations);
        assertFalse(scheduler.fixedFuture.cancelled);
    }

    @Test
    void tick_shouldKeepRunning_afterPublisherThrows() {
        ToggleablePublisher toggleable = new ToggleablePublisher();
        HeartbeatService service = new HeartbeatService(toggleable, scheduler, "1.2.3", clock());

        service.start("server-uuid", "nexus.heartbeat");
        toggleable.shouldThrow = true;
        scheduler.runFixedDelayOnce();
        toggleable.shouldThrow = false;
        scheduler.runFixedDelayOnce();

        assertEquals(1, toggleable.successes);
    }

    private Supplier<Instant> clock() {
        return new Supplier<Instant>() {
            @Override
            public Instant get() {
                return now.get();
            }
        };
    }

    private static final class RecordingPublisher implements HeartbeatService.KafkaPublisher {
        final List<Call> calls = new ArrayList<Call>();

        @Override
        public void send(String topic, String key, Object payload) {
            calls.add(new Call(topic, key, payload));
        }

        static final class Call {
            final String topic;
            final String key;
            final Object payload;

            Call(String topic, String key, Object payload) {
                this.topic = topic;
                this.key = key;
                this.payload = payload;
            }
        }
    }

    private static final class ThrowingPublisher implements HeartbeatService.KafkaPublisher {
        int invocations = 0;

        @Override
        public void send(String topic, String key, Object payload) {
            invocations++;
            throw new RuntimeException("simulated publish failure");
        }
    }

    private static final class ToggleablePublisher implements HeartbeatService.KafkaPublisher {
        boolean shouldThrow = false;
        int successes = 0;

        @Override
        public void send(String topic, String key, Object payload) {
            if (shouldThrow) {
                throw new RuntimeException("simulated publish failure");
            }
            successes++;
        }
    }
}
