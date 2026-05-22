package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import app.l2nx.gs.adapter.api.domain.item.ItemAttribute;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeLineTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemId(57L)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .attributes(null)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemId(57L)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<ItemAttribute, Integer> source = new HashMap<ItemAttribute, Integer>();
        source.put(ItemAttribute.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .attributes(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemId(57L)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> line.getAttributes().put(ItemAttribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<ItemAttribute, Integer> source = new HashMap<ItemAttribute, Integer>();
        source.put(ItemAttribute.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemId(1234L)
                .attributes(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemId(57L)
                .build();

        source.put(ItemAttribute.WATER, 150);

        assertEquals(1, line.getAttributes().size());
        assertEquals(Integer.valueOf(300), line.getAttributes().get(ItemAttribute.FIRE));
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
        Map<ItemAttribute, Integer> attrs = new LinkedHashMap<ItemAttribute, Integer>();
        attrs.put(ItemAttribute.FIRE, 300);
        attrs.put(ItemAttribute.HOLY, 150);

        TradeLine original = TradeLine.builder()
                .itemId(6364L)
                .enchantLevel(Integer.valueOf(16))
                .attributes(attrs)
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
    void equals_shouldDistinguishAttributes() {
        TradeLine a = TradeLine.builder().itemId(1L).count(1L).unitPrice(1L).currencyItemId(57L)
                .attributes(java.util.Collections.singletonMap(ItemAttribute.FIRE, 300))
                .build();
        TradeLine b = TradeLine.builder().itemId(1L).count(1L).unitPrice(1L).currencyItemId(57L)
                .attributes(java.util.Collections.singletonMap(ItemAttribute.WATER, 300))
                .build();

        assertNotEquals(a, b);
    }
}
