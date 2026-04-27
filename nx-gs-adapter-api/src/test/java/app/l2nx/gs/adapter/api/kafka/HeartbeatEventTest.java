package app.l2nx.gs.adapter.api.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HeartbeatEventTest {

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        HeartbeatEvent original = HeartbeatEvent.builder()
                .serverId("00000000-0000-0000-0000-000000000001")
                .adapterVersion("1.2.3")
                .uptime(42L)
                .build();

        HeartbeatEvent copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        HeartbeatEvent event = HeartbeatEvent.builder()
                .serverId("server-id")
                .adapterVersion("0.1.0")
                .uptime(123L)
                .build();

        assertEquals("server-id", event.getServerId());
        assertEquals("0.1.0", event.getAdapterVersion());
        assertEquals(123L, event.getUptime());
    }
}
