package app.l2nx.gs.commons.privatestore;

import app.l2nx.gs.commons.hash.Fnv1a64;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Stable, order-independent FNV-1a 64-bit hash over a private-store order
 * book (one {@code (itemId, side)} pair). Used by host-side snapshot daemons
 * to detect whether a pair changed since the previous tick — equal hash
 * across two ticks ⇒ same set of offers ⇒ skip emit.
 *
 * <p>Independence of input order is achieved by canonical-sorting offers
 * before hashing. The sort key chain is
 * {@code (unitPrice, traderId, enchantLevel, count, currencyItemId)}; ties
 * across all five fields are pathological in practice (same trader posting
 * an indistinguishable duplicate offer) so {@code elementalAttrs} is not a
 * tiebreaker.</p>
 *
 * <p>Purpose-built for change-detection — collisions are acceptable in the
 * cryptographic sense (FNV-1a is non-cryptographic). Two genuinely-different
 * offer sets producing the same hash would cause one missed snapshot tick;
 * the next change recovers consistency and the tombstone flow guarantees
 * eventually-empty pairs are observed.</p>
 *
 * <p>Stateless and side-effect-free — invocations are safe to run on a
 * shared daemon thread without coordination.</p>
 */
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
        return Long.compare(a.currencyItemId, b.currencyItemId);
    };

    private static int compareNullableIntegers(@Nullable Integer a, @Nullable Integer b) {
        if (a == null) return b == null ? 0 : -1;
        if (b == null) return 1;
        return Integer.compare(a.intValue(), b.intValue());
    }

    private PrivateStoreOfferHasher() {
    }

    /**
     * Compute the canonical FNV-1a 64-bit hash of an offer set. Empty input
     * yields a stable sentinel hash distinct from any non-empty input.
     *
     * @param offers offer rows for one {@code (itemId, side)} pair; may be
     *               empty (returns the empty-set sentinel) but MUST NOT be
     *               {@code null}
     * @return canonical hash value for change-detection
     */
    public static long hash(List<OfferRow> offers) {
        ArrayList<OfferRow> sorted = new ArrayList<OfferRow>(offers);
        Collections.sort(sorted, CANONICAL_ORDER);

        long h = Fnv1a64.start();
        h = Fnv1a64.mix(h, sorted.size());
        for (OfferRow r : sorted) {
            h = Fnv1a64.mix(h, r.traderId);
            h = mixNullableInteger(h, r.enchantLevel);
            h = mixElementalAttrs(h, r.elementalAttrs);
            h = Fnv1a64.mix(h, r.count);
            h = Fnv1a64.mix(h, r.unitPrice);
            h = Fnv1a64.mix(h, r.currencyItemId);
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

    private static long mixElementalAttrs(long state, @Nullable Map<String, Integer> attrs) {
        if (attrs == null || attrs.isEmpty()) {
            return Fnv1a64.mix(state, 0);
        }
        long h = Fnv1a64.mix(state, attrs.size());
        ArrayList<String> keys = new ArrayList<String>(attrs.keySet());
        Collections.sort(keys);
        for (String k : keys) {
            h = Fnv1a64.mix(h, k);
            Integer v = attrs.get(k);
            h = Fnv1a64.mix(h, v == null ? 0 : v.intValue());
        }
        return h;
    }
}
