package app.l2nx.gs.commons.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Plain data holder for {@link PrivateStoreOfferHasher}. Mirrors the wire
 * {@code Offer} field set; public final fields, no defensive copying —
 * caller must not mutate the row after handing it to the hasher.
 */
public final class OfferRow {

    public final long traderId;
    public final @Nullable Integer enchantLevel;
    public final @Nullable Map<Attribute, Integer> attributes;
    public final long count;
    public final long unitPrice;
    public final long currencyItemId;
    public final boolean packaged;

    public OfferRow(
            long traderId,
            @Nullable Integer enchantLevel,
            @Nullable Map<Attribute, Integer> attributes,
            long count,
            long unitPrice,
            long currencyItemId,
            boolean packaged) {
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.attributes = attributes;
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
        this.packaged = packaged;
    }
}
