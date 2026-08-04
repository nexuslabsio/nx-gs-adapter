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
 *   <li>{@link #SOURCE} — how the deal was initiated: absent or
 *   {@link #SOURCE_IN_GAME} for the in-game store packet,
 *   {@link #SOURCE_REMOTE} for a platform-issued buy command.</li>
 *   <li>{@link #TAX_ADENA} — buyer-side surcharge burned on this deal, as a
 *   decimal string. Not part of the seller's proceeds — the purchase lines
 *   already carry those.</li>
 *   <li>{@link #TAX_PERCENT} — the rate {@link #TAX_ADENA} was computed at, in
 *   whole percent. Recorded per deal because the rate is platform
 *   configuration and changes over time.</li>
 * </ul>
 */
public final class WellKnownPrivateStoreMetadata {

    private WellKnownPrivateStoreMetadata() {}

    public static final String STORE_OWNER_ADENA = "store_owner_adena";

    public static final String SOURCE = "source";

    public static final String TAX_ADENA = "tax_adena";

    public static final String TAX_PERCENT = "tax_percent";

    /** {@link #SOURCE} value for a deal closed by the in-game store packet. */
    public static final String SOURCE_IN_GAME = "in_game";

    /** {@link #SOURCE} value for a deal closed by a platform buy command. */
    public static final String SOURCE_REMOTE = "remote";
}
