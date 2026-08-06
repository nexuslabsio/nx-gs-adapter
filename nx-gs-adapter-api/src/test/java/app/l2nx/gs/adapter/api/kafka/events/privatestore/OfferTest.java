package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OfferTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        Offer offer = Offer.builder()
                .traderId(42L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();

        assertTrue(offer.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        Offer offer = Offer.builder()
                .traderId(42L)
                .attributes(null)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();

        assertTrue(offer.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L)
                .attributes(source)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();

        assertThrows(
                UnsupportedOperationException.class, () -> offer.getAttributes().put(Attribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        Offer offer = Offer.builder()
                .traderId(42L)
                .attributes(source)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();

        source.put(Attribute.WATER, 150);

        assertEquals(1, offer.getAttributes().size());
        assertEquals(Integer.valueOf(300), offer.getAttributes().get(Attribute.FIRE));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<Attribute, Integer> attrs = new LinkedHashMap<Attribute, Integer>();
        attrs.put(Attribute.FIRE, 300);

        Offer original = Offer.builder()
                .traderId(268437521L)
                .enchantLevel(8)
                .attributes(attrs)
                .count(2L)
                .unitPrice(50_000_000L)
                .currencyItemTemplateId(57L)
                .build();

        Offer copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishTraderId() {
        Offer a = Offer.builder()
                .traderId(1L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();
        Offer b = Offer.builder()
                .traderId(2L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishAttributes() {
        Offer a = Offer.builder()
                .traderId(1L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .attributes(Collections.singletonMap(Attribute.FIRE, 300))
                .build();
        Offer b = Offer.builder()
                .traderId(1L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .attributes(Collections.singletonMap(Attribute.FIRE, 301))
                .build();

        assertNotEquals(a, b);
    }
}
