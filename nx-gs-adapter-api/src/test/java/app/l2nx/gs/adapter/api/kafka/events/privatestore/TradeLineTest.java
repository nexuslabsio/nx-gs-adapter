package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeLineTest {

    @Test
    void getElementalAttrs_shouldReturnEmptyMap_whenBuilderOmits() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemId(57L)
                .build();

        assertTrue(line.getElementalAttrs().isEmpty());
    }

    @Test
    void getElementalAttrs_shouldReturnEmptyMap_whenBuilderPassesNull() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .elementalAttrs(null)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemId(57L)
                .build();

        assertTrue(line.getElementalAttrs().isEmpty());
    }

    @Test
    void getElementalAttrs_shouldBeUnmodifiable() {
        Map<String, Integer> source = new HashMap<String, Integer>();
        source.put(WellKnownElements.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .elementalAttrs(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemId(57L)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> line.getElementalAttrs().put(WellKnownElements.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyElementalAttrs() {
        Map<String, Integer> source = new HashMap<String, Integer>();
        source.put(WellKnownElements.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .elementalAttrs(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemId(57L)
                .build();

        source.put(WellKnownElements.WATER, 150);

        assertEquals(1, line.getElementalAttrs().size());
        assertEquals(Integer.valueOf(300), line.getElementalAttrs().get(WellKnownElements.FIRE));
    }

    @Test
    void enchantLevel_shouldDefaultToNull_signalingNotApplicable() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L).count(1L).unitPrice(1L).currencyItemId(57L)
                .build();

        assertNull(line.getEnchantLevel());
    }

    @Test
    void enchantLevel_shouldRoundtripExplicitZero() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L).enchantLevel(0)
                .count(1L).unitPrice(1L).currencyItemId(57L)
                .build();

        assertEquals(Integer.valueOf(0), line.getEnchantLevel());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<String, Integer> attrs = new LinkedHashMap<String, Integer>();
        attrs.put(WellKnownElements.FIRE, 300);
        attrs.put(WellKnownElements.HOLY, 150);

        TradeLine original = TradeLine.builder()
                .itemId(6364L)
                .enchantLevel(Integer.valueOf(16))
                .elementalAttrs(attrs)
                .count(1L)
                .unitPrice(1_500_000_000L)
                .currencyItemId(57L)
                .build();

        TradeLine copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishUnitPrice() {
        TradeLine a = TradeLine.builder().itemId(1L).count(1L).unitPrice(100L).currencyItemId(57L).build();
        TradeLine b = TradeLine.builder().itemId(1L).count(1L).unitPrice(101L).currencyItemId(57L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishElementalAttrs() {
        TradeLine a = TradeLine.builder().itemId(1L).count(1L).unitPrice(1L).currencyItemId(57L)
                .elementalAttrs(java.util.Collections.singletonMap(WellKnownElements.FIRE, 300))
                .build();
        TradeLine b = TradeLine.builder().itemId(1L).count(1L).unitPrice(1L).currencyItemId(57L)
                .elementalAttrs(java.util.Collections.singletonMap(WellKnownElements.WATER, 300))
                .build();

        assertNotEquals(a, b);
    }
}
