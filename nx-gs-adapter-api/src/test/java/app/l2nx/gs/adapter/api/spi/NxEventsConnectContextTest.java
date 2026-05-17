package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NxEventsConnectContextTest {

    @Test
    void events_shouldDefaultToNoOp_whenBuilderOmits() {
        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary")
                .serverName("Acme").adapterVersion("0.13.0")
                .syncTopics(new SyncTopics(null, null, null))
                .build();

        // No NPE, no throw — and the same singleton across calls.
        assertNotNull(ctx.events());
        ctx.events().publishPremiumPurchase(stubPurchase());
    }

    @Test
    void events_shouldReturnProvidedImpl_whenBuilderSets() {
        AtomicReference<PremiumPurchaseEvent> seen = new AtomicReference<PremiumPurchaseEvent>();
        NxEvents capturing = new NxEvents() {
            @Override
            public void publishPremiumPurchase(PremiumPurchaseEvent event) {
                seen.set(event);
            }

            @Override
            public void publishServerOnlineSnapshot(ServerOnlineSnapshotEvent event) {
            }

            @Override
            public void publishPrivateStoreSnapshot(PrivateStoreSnapshotEvent event) {
            }

            @Override
            public void publishPrivateStorePurchase(PrivateStorePurchaseEvent event) {
            }

            @Override
            public void publishCharacterPresence(CharacterPresenceEvent event) {
            }
        };

        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary")
                .serverName("Acme").adapterVersion("0.13.0")
                .events(capturing)
                .build();

        PremiumPurchaseEvent event = stubPurchase();
        ctx.events().publishPremiumPurchase(event);

        assertSame(event, seen.get());
    }

    @Test
    void equals_shouldIgnoreEventsCapability() {
        UUID tenant = UUID.randomUUID();
        UUID server = UUID.randomUUID();

        ConnectContext withNoOp = ConnectContext.builder()
                .tenantId(tenant).tenantSlug("acme")
                .serverId(server).serverSlug("primary")
                .serverName("Acme").adapterVersion("0.13.0")
                .build();
        ConnectContext withCustom = withNoOp.toBuilder()
                .events(new NxEvents() {
                    @Override
                    public void publishPremiumPurchase(PremiumPurchaseEvent event) {
                    }

                    @Override
                    public void publishServerOnlineSnapshot(ServerOnlineSnapshotEvent event) {
                    }

                    @Override
                    public void publishPrivateStoreSnapshot(PrivateStoreSnapshotEvent event) {
                    }

                    @Override
                    public void publishPrivateStorePurchase(PrivateStorePurchaseEvent event) {
                    }

                    @Override
                    public void publishCharacterPresence(CharacterPresenceEvent event) {
                    }
                })
                .build();

        // Identity bits identical → equal even though events impls differ.
        assertEquals(withNoOp, withCustom);
        assertEquals(withNoOp.hashCode(), withCustom.hashCode());
    }

    private static PremiumPurchaseEvent stubPurchase() {
        return PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .build();
    }
}
