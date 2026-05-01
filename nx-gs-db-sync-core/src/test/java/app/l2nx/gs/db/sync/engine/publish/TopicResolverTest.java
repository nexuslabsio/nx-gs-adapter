package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.spi.ConnectContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TopicResolverTest {

    @Test
    void fromSnapshot_shouldReturnTopic_forKnownEntity() {
        TopicResolver resolver = TopicResolver.fromSnapshot(
                Collections.singletonMap("clan", "bohpts.gs.sync.clans"));

        assertEquals("bohpts.gs.sync.clans", resolver.resolveTopic("clan"));
    }

    @Test
    void fromSnapshot_shouldReturnNull_forMissingEntity() {
        TopicResolver resolver = TopicResolver.fromSnapshot(
                Collections.singletonMap("clan", "bohpts.gs.sync.clans"));

        assertNull(resolver.resolveTopic("character"));
    }

    @Test
    void fromSnapshot_shouldReturnNull_whenSourceNull() {
        TopicResolver resolver = TopicResolver.fromSnapshot(null);

        assertNull(resolver.resolveTopic("clan"));
    }

    @Test
    void fromSnapshot_shouldReturnNull_whenSourceEmpty() {
        TopicResolver resolver = TopicResolver.fromSnapshot(Collections.emptyMap());

        assertNull(resolver.resolveTopic("clan"));
    }

    @Test
    void fromSnapshot_shouldDefensivelyCopy_whenSourceMutates() {
        Map<String, String> source = new HashMap<String, String>();
        source.put("clan", "bohpts.gs.sync.clans");

        TopicResolver resolver = TopicResolver.fromSnapshot(source);
        source.put("clan", "evil-override");
        source.put("character", "smuggled-in");

        assertEquals("bohpts.gs.sync.clans", resolver.resolveTopic("clan"));
        assertNull(resolver.resolveTopic("character"));
    }

    @Test
    void fromContext_shouldRouteThroughCtxSyncTopicsDb() {
        app.l2nx.gs.adapter.api.rest.SyncTopics topics = app.l2nx.gs.adapter.api.rest.SyncTopics.builder()
                .db(Collections.singletonMap("clan", "bohpts.gs.sync.db.clan"))
                .runtime(Collections.singletonMap("character", "bohpts.gs.sync.runtime.character"))
                .build();
        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("acme-x1")
                .serverName("X1")
                .adapterVersion("1.0.0")
                .syncTopics(topics)
                .build();

        TopicResolver resolver = TopicResolver.fromContext(ctx);

        // Resolves only the db namespace — runtime entries do not leak in.
        assertEquals("bohpts.gs.sync.db.clan", resolver.resolveTopic("clan"));
        assertNull(resolver.resolveTopic("character"));
    }

    @Test
    void fromContext_shouldHandleNullCtx() {
        TopicResolver resolver = TopicResolver.fromContext(null);

        assertNull(resolver.resolveTopic("clan"));
    }
}
