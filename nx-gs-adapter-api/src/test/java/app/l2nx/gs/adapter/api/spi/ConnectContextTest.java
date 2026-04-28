package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectContextTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        UUID tenantId = UUID.randomUUID();
        UUID serverId = UUID.randomUUID();

        ConnectContext ctx = ConnectContext.builder()
                .tenantId(tenantId)
                .tenantSlug("acme")
                .serverId(serverId)
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .build();

        assertEquals(tenantId, ctx.getTenantId());
        assertEquals("acme", ctx.getTenantSlug());
        assertEquals(serverId, ctx.getServerId());
        assertEquals("primary", ctx.getServerSlug());
        assertEquals("Acme Primary", ctx.getServerName());
        assertEquals("0.1.0", ctx.getAdapterVersion());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ConnectContext original = ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
