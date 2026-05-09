package app.l2nx.gs.adapter.api.kafka.events.privatestore;

/**
 * Marker base for {@code events.private_store} family DTOs. Carries no fields —
 * its sole purpose is the type bound on
 * {@code NxEvents.publishPrivateStore(PrivateStoreEvent)} so future subtypes
 * plug in without changing the publish-side SPI.
 *
 * <p>Concrete subtypes live in the same {@code privatestore} package and are
 * dispatched on the platform consumer via the {@code Nx-Message-Type} Kafka
 * header (carrying the simple class name).</p>
 *
 * <p>Phase-1 concrete subtypes:</p>
 * <ul>
 *     <li>{@link PrivateStoreTradeEvent} — discrete fact: closed deal pushed
 *     by host hooks at the moment a private-store transaction is finalized
 *     on the game thread.</li>
 *     <li>{@link PrivateStoreSnapshotEvent} — order-book state for one
 *     {@code (itemId, side)} pair, pushed by a host-managed daemon on a
 *     configured cadence with change-detection.</li>
 * </ul>
 */
public abstract class PrivateStoreEvent {

    /**
     * Constructor visibility is {@code protected} — subclassing is the only
     * supported extension model.
     */
    protected PrivateStoreEvent() {
    }
}
