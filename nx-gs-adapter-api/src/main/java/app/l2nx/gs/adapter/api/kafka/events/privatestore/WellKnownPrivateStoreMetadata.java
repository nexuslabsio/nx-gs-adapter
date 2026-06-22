package app.l2nx.gs.adapter.api.kafka.events.privatestore;

/**
 * Canonical key constants for the {@code metadata} map of
 * {@link PrivateStorePurchaseEvent}. The map is open — hosts MAY publish
 * arbitrary additional keys; consumers treat unknown keys as opaque. Adding a
 * constant here is a non-breaking minor-version change.
 *
 * <ul>
 *   <li>{@link #STORE_OWNER_ADENA} — the store-opener's adena balance
 *   <b>after</b> the deal closed, as a decimal string. The store-opener is the
 *   notification recipient — the seller for an {@link PrivateStoreSide#ASK ASK}
 *   (sell) store, the buyer for a {@link PrivateStoreSide#BID BID} (buy) store.
 *   Carried so a consumer can show the recipient's current adena immediately,
 *   without waiting for the delayed CDC character sync.</li>
 * </ul>
 */
public final class WellKnownPrivateStoreMetadata {

    private WellKnownPrivateStoreMetadata() {}

    public static final String STORE_OWNER_ADENA = "store_owner_adena";
}
