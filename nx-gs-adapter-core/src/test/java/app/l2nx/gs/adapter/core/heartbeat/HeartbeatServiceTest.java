package app.l2nx.gs.adapter.core.heartbeat;

import app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.core.concurrent.CapturingScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatServiceTest {

    private CapturingScheduler scheduler;
    private RecordingPublisher publisher;
    private AtomicReference<Instant> now;
    private AtomicReference<List<ModuleStatus>> moduleStatuses;

    @BeforeEach
    void setUp() {
        scheduler = new CapturingScheduler();
        publisher = new RecordingPublisher();
        now = new AtomicReference<Instant>(Instant.parse("2026-01-01T00:00:00Z"));
        moduleStatuses = new AtomicReference<List<ModuleStatus>>(Collections.emptyList());
    }

    @Test
    void start_shouldFireImmediatelyAndThenEvery60Seconds() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());

        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");

        assertNotNull(scheduler.fixedDelayRunnable);
        assertEquals(0L, scheduler.fixedInitialDelay);
        assertEquals(60L, scheduler.fixedPeriod);
        assertEquals(TimeUnit.SECONDS, scheduler.fixedUnit);
    }

    @Test
    void start_shouldPublishPayload_onTick() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());
        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");

        now.set(now.get().plusSeconds(60));
        scheduler.runFixedDelayOnce();

        assertEquals(1, publisher.calls.size());
        RecordingPublisher.Call call = publisher.calls.get(0);
        assertEquals("nexus.heartbeat", call.topic);
        assertEquals("server-uuid", call.key);
        HeartbeatEvent event = (HeartbeatEvent) call.payload;
        assertEquals("tenant-uuid", event.getTenantId());
        assertEquals("acme", event.getTenantSlug());
        assertEquals("server-uuid", event.getServerId());
        assertEquals("primary", event.getServerSlug());
        assertEquals("Acme Primary", event.getServerName());
        assertEquals("1.2.3", event.getAdapterVersion());
        assertEquals(Duration.ofMinutes(1), event.getUptime());
    }

    @Test
    void tick_shouldIncludeModuleStatuses_inPayload() {
        ModuleStatus dbSync = ModuleStatus.builder().name("db-sync").state("ACTIVE").build();
        moduleStatuses.set(Collections.singletonList(dbSync));
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());
        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");

        scheduler.runFixedDelayOnce();

        HeartbeatEvent event = (HeartbeatEvent) publisher.calls.get(0).payload;
        assertEquals(Collections.singletonList(dbSync), event.getEnabledModules());
    }

    @Test
    void tick_shouldDegradeToEmptyList_whenStatusSupplierThrows() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3",
                () -> {
                    throw new RuntimeException("simulated");
                }, clock());
        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");

        scheduler.runFixedDelayOnce();

        HeartbeatEvent event = (HeartbeatEvent) publisher.calls.get(0).payload;
        assertEquals(Collections.emptyList(), event.getEnabledModules());
    }

    @Test
    void start_shouldResetUptime_whenCalledTwice() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());

        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");
        CapturingScheduler.CancellableFuture firstFuture = scheduler.fixedFuture;
        now.set(now.get().plusSeconds(300));

        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");
        assertTrue(firstFuture.cancelled);

        now.set(now.get().plusSeconds(30));
        scheduler.runFixedDelayOnce();

        HeartbeatEvent event = (HeartbeatEvent) publisher.calls.get(0).payload;
        // 30s, not 330s — connectInstant was recaptured at the second start().
        assertEquals(Duration.ofSeconds(30), event.getUptime());
    }

    @Test
    void stop_shouldCancelInFlightSchedule() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());
        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");

        service.stop();

        assertTrue(scheduler.fixedFuture.cancelled);
    }

    @Test
    void stop_shouldBeIdempotent_whenNeverStarted() {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());

        service.stop();
        service.stop();
    }

    @Test
    void tick_shouldNotPropagate_whenPublisherThrows() {
        ThrowingPublisher throwing = new ThrowingPublisher();
        HeartbeatService service = new HeartbeatService(throwing, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());

        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");
        scheduler.runFixedDelayOnce();

        // A propagated tick would cancel the scheduled task.
        assertEquals(1, throwing.invocations);
        assertFalse(scheduler.fixedFuture.cancelled);
    }

    @Test
    void startAndStop_shouldBeMutuallyExclusive_underConcurrentCallers() throws Exception {
        HeartbeatService service = new HeartbeatService(publisher, scheduler, "1.2.3",
                () -> moduleStatuses.get(), clock());

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
            java.util.List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                final boolean doStart = (i % 2 == 0);
                futures.add(pool.submit(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (doStart) {
                        service.start("t", "ts", "s", "ss", "sn", "topic");
                    } else {
                        service.stop();
                    }
                }));
            }
            gate.countDown();
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        // Service must remain in a coherent state — no exception leaked, final
        // stop quiesces cleanly.
        service.stop();
    }

    @Test
    void tick_shouldKeepRunning_afterPublisherThrows() {
        ToggleablePublisher toggleable = new ToggleablePublisher();
        HeartbeatService service = new HeartbeatService(toggleable, scheduler, "1.2.3", () -> moduleStatuses.get(), clock());

        service.start("tenant-uuid", "acme", "server-uuid", "primary", "Acme Primary", "nexus.heartbeat");
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
