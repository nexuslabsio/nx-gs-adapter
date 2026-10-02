package app.l2nx.gs.commons.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import app.l2nx.gs.commons.hash.Fnv1a64;
import java.util.*;
import org.jspecify.annotations.Nullable;

/** Order-independent; collisions are rare and cost at most one missed change-detection tick. */
public final class PrivateStoreOfferHasher {

    private static final Comparator<OfferRow> CANONICAL_ORDER = (a, b) -> {
        int c = Long.compare(a.unitPrice, b.unitPrice);
        if (c != 0) return c;
        c = Long.compare(a.traderId, b.traderId);
        if (c != 0) return c;
        c = compareNullableIntegers(a.enchantLevel, b.enchantLevel);
        if (c != 0) return c;
        c = Long.compare(a.count, b.count);
        if (c != 0) return c;
        c = Long.compare(a.currencyItemTemplateId, b.currencyItemTemplateId);
        if (c != 0) return c;
        // itemId is unique per instance, so ties are impossible past this point
        return Long.compare(a.itemId, b.itemId);
    };

    private static int compareNullableIntegers(@Nullable Integer a, @Nullable Integer b) {
        if (a == null) return b == null ? 0 : -1;
        if (b == null) return 1;
        return Integer.compare(a.intValue(), b.intValue());
    }

    private PrivateStoreOfferHasher() {}

    public static long hash(List<OfferRow> offers) {
        ArrayList<OfferRow> sorted = new ArrayList<OfferRow>(offers);
        Collections.sort(sorted, CANONICAL_ORDER);

        long h = Fnv1a64.start();
        h = Fnv1a64.mix(h, sorted.size());
        for (OfferRow r : sorted) {
            // Hashed so a sold-then-relisted twin (different instance) changes the hash and republishes
            h = Fnv1a64.mix(h, r.itemId);
            h = Fnv1a64.mix(h, r.traderId);
            h = mixNullableInteger(h, r.enchantLevel);
            h = mixAttributes(h, r.attributes);
            h = Fnv1a64.mix(h, r.count);
            h = Fnv1a64.mix(h, r.unitPrice);
            h = Fnv1a64.mix(h, r.currencyItemTemplateId);
            // Hashed so a SELL<->PACKAGE_SELL re-seat with identical items/prices republishes
            h = Fnv1a64.mix(h, r.packaged);
        }
        return h;
    }

    private static long mixNullableInteger(long state, @Nullable Integer value) {
        if (value == null) {
            return Fnv1a64.mix(state, 0);
        }
        long h = Fnv1a64.mix(state, 1);
        return Fnv1a64.mix(h, value.intValue());
    }

    private static final Attribute[] ATTR_ORDER = Attribute.values();

    private static long mixAttributes(long state, @Nullable Map<Attribute, Integer> attrs) {
        if (attrs == null || attrs.isEmpty()) {
            return Fnv1a64.mix(state, 0);
        }
        long h = Fnv1a64.mix(state, attrs.size());
        for (Attribute k : ATTR_ORDER) {
            Integer v = attrs.get(k);
            if (v == null) continue;
            h = Fnv1a64.mix(h, k.ordinal());
            h = Fnv1a64.mix(h, v.intValue());
        }
        return h;
    }
}
