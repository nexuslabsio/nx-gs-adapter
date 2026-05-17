package app.l2nx.gs.adapter.api.kafka.ops;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatEventTest {

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        ModuleStatus dbSync = ModuleStatus.builder()
                .name("db-sync")
                .state("ACTIVE")
                .stats(ModuleStatus.Stats.builder().pool(new PoolStats(1, 3, 4, null)).build())
                .build();

        HeartbeatEvent original = HeartbeatEvent.builder()
                .tenantId("11111111-1111-1111-1111-111111111111")
                .tenantSlug("acme")
                .serverId("00000000-0000-0000-0000-000000000001")
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("1.2.3")
                .uptime(Duration.ofMillis(42))
                .enabledModules(Collections.singletonList(dbSync))
                .build();

        HeartbeatEvent copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        ModuleStatus a = ModuleStatus.builder().name("a").state("ACTIVE").build();
        ModuleStatus b = ModuleStatus.builder().name("b").state("DEGRADED").build();

        HeartbeatEvent event = HeartbeatEvent.builder()
                .tenantId("tenant-id")
                .tenantSlug("tenant-slug")
                .serverId("server-id")
                .serverSlug("server-slug")
                .serverName("Server Name")
                .adapterVersion("0.1.0")
                .uptime(Duration.ofSeconds(123))
                .enabledModules(Arrays.asList(a, b))
                .build();

        assertEquals("tenant-id", event.getTenantId());
        assertEquals("tenant-slug", event.getTenantSlug());
        assertEquals("server-id", event.getServerId());
        assertEquals("server-slug", event.getServerSlug());
        assertEquals("Server Name", event.getServerName());
        assertEquals("0.1.0", event.getAdapterVersion());
        assertEquals(Duration.ofSeconds(123), event.getUptime());
        assertEquals(Arrays.asList(a, b), event.getEnabledModules());
    }

    @Test
    void enabledModules_shouldDefaultToEmptyList_whenBuilderOmitsIt() {
        HeartbeatEvent event = HeartbeatEvent.builder()
                .tenantId("t").tenantSlug("ts")
                .serverId("s").serverSlug("ss").serverName("sn")
                .adapterVersion("v").uptime(Duration.ZERO)
                .build();

        assertEquals(Collections.emptyList(), event.getEnabledModules());
    }

    @Test
    void enabledModules_shouldBeUnmodifiable() {
        ModuleStatus a = ModuleStatus.builder().name("a").state("ACTIVE").build();
        HeartbeatEvent event = HeartbeatEvent.builder()
                .enabledModules(Collections.singletonList(a))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getEnabledModules().add(a));
    }

    @Test
    void enabledModules_shouldDefensivelyCopy_whenSourceMutates() {
        ModuleStatus a = ModuleStatus.builder().name("a").state("ACTIVE").build();
        ModuleStatus b = ModuleStatus.builder().name("b").state("ACTIVE").build();
        java.util.ArrayList<ModuleStatus> source = new java.util.ArrayList<ModuleStatus>();
        source.add(a);

        HeartbeatEvent event = HeartbeatEvent.builder().enabledModules(source).build();
        source.add(b); // mutate after build

        List<ModuleStatus> seen = event.getEnabledModules();
        assertEquals(1, seen.size());
        assertTrue(seen.contains(a));
    }
}
