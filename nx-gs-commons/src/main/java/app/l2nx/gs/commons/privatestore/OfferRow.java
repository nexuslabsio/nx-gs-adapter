package app.l2nx.gs.commons.privatestore;

import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Plain data holder for one private-store offer fed to
 * {@link PrivateStoreOfferHasher#hash(java.util.List)}. Lives in commons —
 * decoupled from the {@code nx-gs-adapter-api} wire {@code Offer} class so
 * the hasher can be invoked over any host-side intermediate representation
 * without dragging the api jar onto every {@code commons} consumer.
 *
 * <p>Field order matches the conceptual reading order: WHO is offering
 * ({@code traderId}), WITH WHAT modifiers ({@code enchantLevel},
 * {@code elementalAttrs}), HOW MUCH ({@code count}), AT WHAT PRICE
 * ({@code unitPrice}, {@code currencyItemId}).</p>
 *
 * <p>Public final fields — this is plumbing, not a wire DTO. No builder, no
 * defensive copying. The hasher reads each field once; mutating the row
 * after passing it to {@code hash(...)} is the caller's responsibility to
 * avoid.</p>
 */
public final class OfferRow {

    public final long traderId;
    public final @Nullable Integer enchantLevel;
    public final @Nullable Map<String, Integer> elementalAttrs;
    public final long count;
    public final long unitPrice;
    public final long currencyItemId;

    public OfferRow(long traderId,
                    @Nullable Integer enchantLevel,
                    @Nullable Map<String, Integer> elementalAttrs,
                    long count,
                    long unitPrice,
                    long currencyItemId) {
        this.traderId = traderId;
        this.enchantLevel = enchantLevel;
        this.elementalAttrs = elementalAttrs;
        this.count = count;
        this.unitPrice = unitPrice;
        this.currencyItemId = currencyItemId;
    }
}
