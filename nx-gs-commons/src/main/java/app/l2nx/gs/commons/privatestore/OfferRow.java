package app.l2nx.gs.commons.privatestore;

import app.l2nx.gs.adapter.api.domain.item.ItemAttribute;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Plain data holder for {@link PrivateStoreOfferHasher}. Mirrors the wire
 * {@code Offer} field set; public final fields, no defensive copying —
 * caller must not mutate the row after handing it to the hasher.
 */
public final class OfferRow {

    public final long traderId;
    public final @Nullable Integer enchantLevel;
    public final @Nullable Map<ItemAttribute, Integer> attributes;
    public final long count;
    public final long unitPrice;
    public final long currencyItemId;

    public OfferRow(long traderId,
                    @Nullable Integer enchantLevel,
                    @Nullable Map<ItemAttribute, Integer> attributes,
                    long count,
                    long unitPrice,
                    long currencyItemId) {
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.attributes = attributes;
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
    }
}
