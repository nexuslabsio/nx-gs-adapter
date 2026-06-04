package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.rest.SyncTopics;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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
    void syncTopics_shouldDefaultToEmpty_whenBuilderOmitsIt() {
        ConnectContext ctx = ConnectContext.builder().build();

        assertNotNull(ctx.getSyncTopics());
        assertTrue(ctx.getSyncTopics().getDb().isEmpty());
        assertTrue(ctx.getSyncTopics().getRuntime().isEmpty());
        assertTrue(ctx.getSyncTopics().getGd().isEmpty());
    }

    @Test
    void syncTopics_shouldNormalizeNullToEmpty() {
        ConnectContext ctx = ConnectContext.builder().syncTopics(null).build();

        assertNotNull(ctx.getSyncTopics());
        assertTrue(ctx.getSyncTopics().getDb().isEmpty());
        assertTrue(ctx.getSyncTopics().getRuntime().isEmpty());
        assertTrue(ctx.getSyncTopics().getGd().isEmpty());
    }

    @Test
    void syncTopics_shouldExposeNamespaces_whenBuilderProvidesIt() {
        SyncTopics topics = SyncTopics.builder()
                .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .runtime(Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"))
                .build();

        ConnectContext ctx = ConnectContext.builder().syncTopics(topics).build();

        assertEquals("bohpts.gs.sync.db.clan", ctx.getSyncTopics().getDb().get("clan"));
        assertEquals("bohpts.gs.sync.runtime.character",
                ctx.getSyncTopics().getRuntime().get("character"));
        assertTrue(ctx.getSyncTopics().getGd().isEmpty());
    }

    @Test
    void syncTopics_namespacesShouldBeUnmodifiable() {
        SyncTopics topics = SyncTopics.builder()
                .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .build();
        ConnectContext ctx = ConnectContext.builder().syncTopics(topics).build();

        assertThrows(UnsupportedOperationException.class,
                () -> ctx.getSyncTopics().getDb().put("character", "x"));
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
                .syncTopics(SyncTopics.builder()
                        .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                        .build())
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
