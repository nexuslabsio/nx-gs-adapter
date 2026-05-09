package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.events.online.OnlineEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
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
        ctx.events().publishPremium(stubPurchase());
    }

    @Test
    void events_shouldReturnProvidedImpl_whenBuilderSets() {
        AtomicReference<PremiumEvent> seen = new AtomicReference<PremiumEvent>();
        NxEvents capturing = new NxEvents() {
            @Override
            public void publishPremium(PremiumEvent event) {
                seen.set(event);
            }

            @Override
            public void publishOnline(OnlineEvent event) {
            }
        };

        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID()).tenantSlug("acme")
                .serverId(UUID.randomUUID()).serverSlug("primary")
                .serverName("Acme").adapterVersion("0.13.0")
                .events(capturing)
                .build();

        PremiumPurchaseEvent event = stubPurchase();
        ctx.events().publishPremium(event);

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
                    public void publishPremium(PremiumEvent event) {
                    }

                    @Override
                    public void publishOnline(OnlineEvent event) {
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
