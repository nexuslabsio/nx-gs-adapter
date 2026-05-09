package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OfferTest {

    @Test
    void getElementalAttrs_shouldReturnEmptyMap_whenBuilderOmits() {
        Offer offer = Offer.builder()
                .traderId(42L).count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertTrue(offer.getElementalAttrs().isEmpty());
    }

    @Test
    void getElementalAttrs_shouldReturnEmptyMap_whenBuilderPassesNull() {
        Offer offer = Offer.builder()
                .traderId(42L).elementalAttrs(null)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertTrue(offer.getElementalAttrs().isEmpty());
    }

    @Test
    void getElementalAttrs_shouldBeUnmodifiable() {
        Map<String, Integer> source = new HashMap<String, Integer>();
        source.put(WellKnownElements.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L).elementalAttrs(source)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> offer.getElementalAttrs().put(WellKnownElements.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyElementalAttrs() {
        Map<String, Integer> source = new HashMap<String, Integer>();
        source.put(WellKnownElements.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L).elementalAttrs(source)
                .count(1L).unitPrice(100L).currencyItemId(57L)
                .build();

        source.put(WellKnownElements.WATER, 150);

        assertEquals(1, offer.getElementalAttrs().size());
        assertEquals(Integer.valueOf(300), offer.getElementalAttrs().get(WellKnownElements.FIRE));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<String, Integer> attrs = new LinkedHashMap<String, Integer>();
        attrs.put(WellKnownElements.FIRE, 300);

        Offer original = Offer.builder()
                .traderId(268437521L)
                .enchantLevel(8)
                .elementalAttrs(attrs)
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
    void equals_shouldDistinguishElementalAttrs() {
        Offer a = Offer.builder()
                .traderId(1L).count(1L).unitPrice(100L).currencyItemId(57L)
                .elementalAttrs(Collections.singletonMap(WellKnownElements.FIRE, 300))
                .build();
        Offer b = Offer.builder()
                .traderId(1L).count(1L).unitPrice(100L).currencyItemId(57L)
                .elementalAttrs(Collections.singletonMap(WellKnownElements.FIRE, 301))
                .build();

        assertNotEquals(a, b);
    }
}
