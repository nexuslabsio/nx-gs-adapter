package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    void syncTopics_shouldDefaultToEmptyMap_whenBuilderOmitsIt() {
        ConnectContext ctx = ConnectContext.builder().build();

        assertTrue(ctx.getSyncTopics().isEmpty());
    }

    @Test
    void syncTopics_shouldNormalizeNullToEmptyMap() {
        ConnectContext ctx = ConnectContext.builder().syncTopics(null).build();

        assertTrue(ctx.getSyncTopics().isEmpty());
    }

    @Test
    void syncTopics_shouldExposeMap_whenBuilderProvidesIt() {
        Map<String, String> topics = new HashMap<String, String>();
        topics.put("clan", "bohpts.gs.sync.clans");

        ConnectContext ctx = ConnectContext.builder().syncTopics(topics).build();

        assertEquals(topics, ctx.getSyncTopics());
    }

    @Test
    void syncTopics_shouldBeUnmodifiable() {
        ConnectContext ctx = ConnectContext.builder()
                .syncTopics(Collections.singletonMap("clan", "bohpts.gs.sync.clans"))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> ctx.getSyncTopics().put("character", "x"));
    }

    @Test
    void syncTopics_shouldDefensivelyCopy_whenSourceMutates() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("clan", "bohpts.gs.sync.clans");

        ConnectContext ctx = ConnectContext.builder().syncTopics(source).build();
        source.put("character", "should-not-leak");

        assertEquals(Collections.singletonMap("clan", "bohpts.gs.sync.clans"),
                ctx.getSyncTopics());
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
                .syncTopics(Collections.singletonMap("clan", "bohpts.gs.sync.clans"))
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
