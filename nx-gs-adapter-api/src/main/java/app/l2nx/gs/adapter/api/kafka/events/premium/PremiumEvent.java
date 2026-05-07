package app.l2nx.gs.adapter.api.kafka.events.premium;

/**
 * Common abstract base for all events in the {@code premium} family. Carries
 * no fields — its sole purpose is the type bound on
 * {@code NxEvents.publishPremium(PremiumEvent)} so future subtypes
 * ({@code PremiumRefundEvent}, {@code PremiumGiftReceivedEvent}, …) plug in
 * without changing the publish-side SPI.
 *
 * <p>Concrete subtypes live in the same {@code premium} package and are
 * dispatched by the platform consumer via the {@code Nx-Message-Type} Kafka
 * header (which carries the simple class name).</p>
 *
 * <p>Phase-1 single concrete subtype: {@link PremiumPurchaseEvent}.</p>
 */
public abstract class PremiumEvent {

    /**
     * Constructor visibility is {@code protected} — subclassing is the only
     * supported extension model.
     */
    protected PremiumEvent() {
    }
}
