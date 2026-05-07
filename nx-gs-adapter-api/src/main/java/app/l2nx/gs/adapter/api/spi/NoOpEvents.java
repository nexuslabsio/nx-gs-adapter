package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumEvent;

/**
 * Default {@link NxEvents} implementation used when {@link ConnectContext} is
 * built without a publisher (e.g. unit tests, contexts constructed before the
 * adapter-core wiring is ready). Every method swallows the call silently.
 *
 * <p>Package-private — host code never references this directly. The contract
 * "publish before connect → no-op + DEBUG log" is intentional; the actual
 * adapter-core implementation produces the DEBUG log when its publisher is
 * not yet ready. This fallback exists so {@code ctx.events().publishPremium(...)}
 * never throws {@code NullPointerException} on a partially-initialized
 * context.</p>
 */
final class NoOpEvents implements NxEvents {

    static final NoOpEvents INSTANCE = new NoOpEvents();

    private NoOpEvents() {
    }

    @Override
    public void publishPremium(PremiumEvent event) {
        // intentional no-op — see class Javadoc
    }
}
