package app.l2nx.gs.commons.privatestore;

import app.l2nx.gs.adapter.api.domain.item.ItemAttribute;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PrivateStoreOfferHasherTest {

    @Test
    void hash_shouldBeIndependentOfInputOrder() {
        OfferRow a = row(1L, 0, null, 1L, 100L, 57L);
        OfferRow b = row(2L, 0, null, 1L, 200L, 57L);
        OfferRow c = row(3L, 0, null, 1L, 300L, 57L);

        long hashAscending = PrivateStoreOfferHasher.hash(Arrays.asList(a, b, c));
        long hashDescending = PrivateStoreOfferHasher.hash(Arrays.asList(c, b, a));
        long hashShuffled = PrivateStoreOfferHasher.hash(Arrays.asList(b, a, c));

        assertEquals(hashAscending, hashDescending);
        assertEquals(hashAscending, hashShuffled);
    }

    @Test
    void hash_shouldChange_whenAddingOffer() {
        OfferRow a = row(1L, 0, null, 1L, 100L, 57L);
        OfferRow b = row(2L, 0, null, 1L, 200L, 57L);

        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(a));
        long h2 = PrivateStoreOfferHasher.hash(Arrays.asList(a, b));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenRemovingOffer() {
        OfferRow a = row(1L, 0, null, 1L, 100L, 57L);
        OfferRow b = row(2L, 0, null, 1L, 200L, 57L);

        long h2 = PrivateStoreOfferHasher.hash(Arrays.asList(a, b));
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(a));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenUnitPriceChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 101L, 57L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenCountChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 5L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 6L, 100L, 57L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenEnchantLevelChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 16, null, 1L, 100L, 57L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldDistinguishNullEnchantFromZeroEnchant() {
        // null = "item type has no enchant concept" (consumable / material);
        // 0 = "enchantable but unenchanted". These MUST hash differently.
        long hashNull = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, null, null, 1L, 100L, 57L)));
        long hashZero = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));

        assertNotEquals(hashNull, hashZero);
    }

    @Test
    void hash_shouldBeStable_acrossNullEnchantInputs() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, null, null, 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, null, null, 1L, 100L, 57L)));

        assertEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenTraderIdChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(2L, 0, null, 1L, 100L, 57L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldChange_whenCurrencyChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 4037L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldBeStable_whenAttributesKeyOrderChanges() {
        Map<ItemAttribute, Integer> linkedAsc = new LinkedHashMap<ItemAttribute, Integer>();
        linkedAsc.put(ItemAttribute.DARK, 100);
        linkedAsc.put(ItemAttribute.FIRE, 300);
        linkedAsc.put(ItemAttribute.WATER, 150);

        Map<ItemAttribute, Integer> linkedDesc = new LinkedHashMap<ItemAttribute, Integer>();
        linkedDesc.put(ItemAttribute.WATER, 150);
        linkedDesc.put(ItemAttribute.FIRE, 300);
        linkedDesc.put(ItemAttribute.DARK, 100);

        long hashAsc = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, linkedAsc, 1L, 100L, 57L)));
        long hashDesc = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, linkedDesc, 1L, 100L, 57L)));

        assertEquals(hashAsc, hashDesc);
    }

    @Test
    void hash_shouldChange_whenAttributesValueChanges() {
        long h1 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, Collections.singletonMap(ItemAttribute.FIRE, 300), 1L, 100L, 57L)));
        long h2 = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, Collections.singletonMap(ItemAttribute.FIRE, 301), 1L, 100L, 57L)));

        assertNotEquals(h1, h2);
    }

    @Test
    void hash_shouldDistinguishNullFromPresentEmptyAttrs() {
        // null and empty are normalized to the same sentinel — a host that
        // sometimes passes empty maps and sometimes nulls must NOT see false
        // positives in change-detection.
        long hashNull = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long hashEmpty = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, new HashMap<ItemAttribute, Integer>(), 1L, 100L, 57L)));

        assertEquals(hashNull, hashEmpty);
    }

    @Test
    void hash_shouldDistinguishEmptyAttrsFromOneZeroValueAttr() {
        long hashEmpty = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        long hashZero = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, Collections.singletonMap(ItemAttribute.FIRE, 0), 1L, 100L, 57L)));

        assertNotEquals(hashEmpty, hashZero);
    }

    @Test
    void hash_shouldYieldSentinel_forEmptyOfferList() {
        long h = PrivateStoreOfferHasher.hash(Collections.emptyList());

        // Stable across invocations.
        assertEquals(h, PrivateStoreOfferHasher.hash(Collections.emptyList()));

        // Distinct from any non-empty hash.
        long oneOffer = PrivateStoreOfferHasher.hash(Collections.singletonList(
                row(1L, 0, null, 1L, 100L, 57L)));
        assertNotEquals(h, oneOffer);
    }

    @Test
    void hash_shouldNotMutateInputList() {
        List<OfferRow> source = new ArrayList<OfferRow>();
        source.add(row(3L, 0, null, 1L, 300L, 57L));
        source.add(row(1L, 0, null, 1L, 100L, 57L));
        source.add(row(2L, 0, null, 1L, 200L, 57L));

        List<OfferRow> snapshot = new ArrayList<OfferRow>(source);
        PrivateStoreOfferHasher.hash(source);

        assertEquals(snapshot, source);
    }

    private static OfferRow row(long traderId, Integer enchantLevel,
                                Map<ItemAttribute, Integer> attrs,
                                long count, long unitPrice, long currencyItemId) {
        return new OfferRow(traderId, enchantLevel, attrs, count, unitPrice, currencyItemId);
    }
}
