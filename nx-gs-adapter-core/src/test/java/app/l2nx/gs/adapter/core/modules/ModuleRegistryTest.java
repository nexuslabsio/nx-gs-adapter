package app.l2nx.gs.adapter.core.modules;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ModuleRegistryTest {

    private final ConnectContext ctx = ConnectContext.builder()
            .tenantId(UUID.randomUUID())
            .tenantSlug("acme")
            .serverId(UUID.randomUUID())
            .serverSlug("primary")
            .serverName("Acme Primary")
            .adapterVersion("0.1.0")
            .build();

    @Test
    void connect_shouldInvokeOnConnectThenStart_inSortedOrder() {
        RecordingModule first = new RecordingModule("a-first");
        RecordingModule second = new RecordingModule("z-second");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(Arrays.asList(second, first));

        registry.connect(ctx);

        assertEquals(
                Arrays.asList("a-first.onConnect", "z-second.onConnect", "a-first.start", "z-second.start"),
                Recorder.events);
    }

    @Test
    void connect_shouldSkipStart_whenOnConnectThrew() {
        ThrowingOnConnectModule failing = new ThrowingOnConnectModule("a");
        RecordingModule healthy = new RecordingModule("b");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(Arrays.asList(failing, healthy));

        registry.connect(ctx);

        // failing.start MUST NOT have been invoked; healthy.start MUST have been invoked.
        assertFalse(failing.startCalled);
        assertTrue(healthy.startCalled);
    }

    @Test
    void shutdown_shouldInvokeStopThenOnDisconnect_inReverseOrder() {
        RecordingModule first = new RecordingModule("a-first");
        RecordingModule second = new RecordingModule("z-second");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(Arrays.asList(first, second));

        registry.shutdown();

        assertEquals(
                Arrays.asList("z-second.stop", "a-first.stop", "z-second.onDisconnect", "a-first.onDisconnect"),
                Recorder.events);
    }

    @Test
    void shutdown_shouldContinue_whenStopThrows() {
        ThrowingOnStopModule first = new ThrowingOnStopModule("a");
        RecordingModule second = new RecordingModule("b");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(Arrays.asList(first, second));

        registry.shutdown();

        // Both onDisconnect calls still fire despite first.stop throwing.
        assertTrue(first.onDisconnectCalled);
        assertTrue(second.onDisconnectCalled);
    }

    @Test
    void currentStatuses_shouldReportFailed_whenLifecycleHookFailed() {
        ThrowingOnConnectModule failing = new ThrowingOnConnectModule("db-sync");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(java.util.Collections.singletonList(failing));
        registry.connect(ctx);

        List<ModuleStatus> statuses = registry.currentStatuses();

        assertEquals(1, statuses.size());
        assertEquals("db-sync", statuses.get(0).getName());
        assertEquals("FAILED", statuses.get(0).getState());
        assertSame(ModuleStatus.Stats.empty(), statuses.get(0).getStats());
    }

    @Test
    void currentStatuses_shouldFallbackToFailed_whenModuleStatusThrows() {
        ThrowingCurrentStatusModule mod = new ThrowingCurrentStatusModule("metrics");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(java.util.Collections.singletonList(mod));

        List<ModuleStatus> statuses = registry.currentStatuses();

        assertEquals("FAILED", statuses.get(0).getState());
    }

    @Test
    void currentStatuses_shouldForwardModuleReport_whenHealthy() {
        ModuleStatus reported = ModuleStatus.builder()
                .name("db-sync")
                .state("ACTIVE")
                .stats(ModuleStatus.Stats.builder()
                        .pool(new PoolStats(1, 3, 4, null))
                        .build())
                .build();
        FixedStatusModule mod = new FixedStatusModule("db-sync", reported);
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(java.util.Collections.singletonList(mod));

        List<ModuleStatus> statuses = registry.currentStatuses();

        assertEquals(reported, statuses.get(0));
    }

    @Test
    void modules_shouldBeSortedByName() {
        RecordingModule c = new RecordingModule("c");
        RecordingModule a = new RecordingModule("a");
        RecordingModule b = new RecordingModule("b");
        ModuleRegistry registry = new ModuleRegistry();
        registry.discoverFrom(Arrays.asList(c, a, b));

        List<AdapterModule> sorted = registry.modules();

        assertEquals(
                Arrays.asList("a", "b", "c"),
                Arrays.asList(
                        sorted.get(0).name(),
                        sorted.get(1).name(),
                        sorted.get(2).name()));
    }

    private static final class Recorder {
        static final List<String> events = new java.util.ArrayList<String>();

        static void record(String name, String hook) {
            events.add(name + "." + hook);
        }
    }

    private static class RecordingModule implements AdapterModule {
        private final String name;
        boolean startCalled;
        boolean onDisconnectCalled;

        RecordingModule(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void onConnect(ConnectContext ctx) {
            Recorder.record(name, "onConnect");
        }

        @Override
        public void start() {
            Recorder.record(name, "start");
            startCalled = true;
        }

        @Override
        public void stop() {
            Recorder.record(name, "stop");
        }

        @Override
        public void onDisconnect() {
            Recorder.record(name, "onDisconnect");
            onDisconnectCalled = true;
        }
    }

    private static final class ThrowingOnConnectModule extends RecordingModule {
        ThrowingOnConnectModule(String name) {
            super(name);
        }

        @Override
        public void onConnect(ConnectContext ctx) {
            throw new RuntimeException("simulated");
        }
    }

    private static final class ThrowingOnStopModule extends RecordingModule {
        ThrowingOnStopModule(String name) {
            super(name);
        }

        @Override
        public void stop() {
            throw new RuntimeException("simulated");
        }
    }

    private static final class ThrowingCurrentStatusModule extends RecordingModule {
        ThrowingCurrentStatusModule(String name) {
            super(name);
        }

        @Override
        public ModuleStatus currentStatus() {
            throw new RuntimeException("simulated");
        }
    }

    private static final class FixedStatusModule extends RecordingModule {
        private final ModuleStatus fixed;

        FixedStatusModule(String name, ModuleStatus fixed) {
            super(name);
            this.fixed = fixed;
        }

        @Override
        public ModuleStatus currentStatus() {
            return fixed;
        }
    }

    @org.junit.jupiter.api.BeforeEach
    void clearRecorder() {
        Recorder.events.clear();
    }
}
