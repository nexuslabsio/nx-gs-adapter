package app.l2nx.gs.adapter.api.kafka.events.privatestore.model;

import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;

/**
 * Canonical keys for the {@code metadata} map of {@link PrivateStorePurchaseEvent}; hosts MAY add others (opaque to consumers).
 * <p>Adding a constant is a non-breaking minor-version change.
 *
 * <ul>
 *   <li>{@link #STORE_OWNER_ADENA} - store-opener's adena balance <b>after</b> the deal (decimal string). The opener is the notification recipient (seller on {@link PrivateStoreSide#ASK ASK}, buyer on {@link PrivateStoreSide#BID BID}); lets a consumer show it without waiting for the delayed CDC sync.</li>
 *   <li>{@link #SOURCE} - absent or {@link #SOURCE_IN_GAME} for the in-game store packet, {@link #SOURCE_REMOTE} for a platform buy command.</li>
 *   <li>{@link #TAX_ADENA} - buyer-side surcharge burned (decimal string); not part of the seller's proceeds.</li>
 *   <li>{@link #TAX_PERCENT} - rate {@link #TAX_ADENA} was computed at, in whole percent; recorded per deal because the rate is platform configuration.</li>
 * </ul>
 */
public final class WellKnownPrivateStoreMetadata {

    private WellKnownPrivateStoreMetadata() {}

    public static final String STORE_OWNER_ADENA = "store_owner_adena";

    public static final String SOURCE = "source";

    public static final String TAX_ADENA = "tax_adena";

    public static final String TAX_PERCENT = "tax_percent";

    public static final String SOURCE_IN_GAME = "in_game";

    public static final String SOURCE_REMOTE = "remote";
}
