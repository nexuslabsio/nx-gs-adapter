package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.item.ItemAttribute;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OfferTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        Offer offer = Offer.builder()
                .traderId(42L).count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertTrue(offer.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        Offer offer = Offer.builder()
                .traderId(42L).attributes(null)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertTrue(offer.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<ItemAttribute, Integer> source = new HashMap<ItemAttribute, Integer>();
        source.put(ItemAttribute.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L).attributes(source)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> offer.getAttributes().put(ItemAttribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<ItemAttribute, Integer> source = new HashMap<ItemAttribute, Integer>();
        source.put(ItemAttribute.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L).attributes(source)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        source.put(ItemAttribute.WATER, 150);

        assertEquals(1, offer.getAttributes().size());
        assertEquals(Integer.valueOf(300), offer.getAttributes().get(ItemAttribute.FIRE));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<ItemAttribute, Integer> attrs = new LinkedHashMap<ItemAttribute, Integer>();
        attrs.put(ItemAttribute.FIRE, 300);

        Offer original = Offer.builder()
                .traderId(268437521L)
                .enchantLevel(8)
                .attributes(attrs)
                .count(2L)
                .unitPrice(50_000_000L)
                .currencyItemId(57L)
                .build();

        Offer copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishTraderId() {
        Offer a = Offer.builder()
                .traderId(1L).count(1L).unitPrice(100L).currencyItemId(57L).build();
        Offer b = Offer.builder()
                .traderId(2L).count(1L).unitPrice(100L).currencyItemId(57L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishAttributes() {
        Offer a = Offer.builder()
                .traderId(1L).count(1L).unitPrice(100L).currencyItemId(57L)
                .attributes(Collections.singletonMap(ItemAttribute.FIRE, 300))
                .build();
        Offer b = Offer.builder()
                .traderId(1L).count(1L).unitPrice(100L).currencyItemId(57L)
                .attributes(Collections.singletonMap(ItemAttribute.FIRE, 301))
                .build();

        assertNotEquals(a, b);
    }
}
