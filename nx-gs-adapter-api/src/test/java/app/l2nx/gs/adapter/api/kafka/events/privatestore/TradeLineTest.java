package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TradeLineTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemTemplateId(57L)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .attributes(null)
                .count(5L)
                .unitPrice(100_000L)
                .currencyItemTemplateId(57L)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .attributes(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .build();

        assertThrows(
                UnsupportedOperationException.class, () -> line.getAttributes().put(Attribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .attributes(source)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .build();

        source.put(Attribute.WATER, 150);

        assertEquals(1, line.getAttributes().size());
        assertEquals(Integer.valueOf(300), line.getAttributes().get(Attribute.FIRE));
    }

    @Test
    void enchantLevel_shouldDefaultToNull_signalingNotApplicable() {
        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .build();

        assertNull(line.getEnchantLevel());
    }

    @Test
    void enchantLevel_shouldRoundtripExplicitZero() {
        TradeLine line = TradeLine.builder()
                .itemTemplateId(1234L)
                .enchantLevel(0)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .build();

        assertEquals(Integer.valueOf(0), line.getEnchantLevel());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<Attribute, Integer> attrs = new LinkedHashMap<Attribute, Integer>();
        attrs.put(Attribute.FIRE, 300);
        attrs.put(Attribute.HOLY, 150);

        TradeLine original = TradeLine.builder()
                .itemTemplateId(6364L)
                .enchantLevel(Integer.valueOf(16))
                .attributes(attrs)
                .count(1L)
                .unitPrice(1_500_000_000L)
                .currencyItemTemplateId(57L)
                .build();

        TradeLine copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishUnitPrice() {
        TradeLine a = TradeLine.builder()
                .itemTemplateId(1L)
                .count(1L)
                .unitPrice(100L)
                .currencyItemTemplateId(57L)
                .build();
        TradeLine b = TradeLine.builder()
                .itemTemplateId(1L)
                .count(1L)
                .unitPrice(101L)
                .currencyItemTemplateId(57L)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishAttributes() {
        TradeLine a = TradeLine.builder()
                .itemTemplateId(1L)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .attributes(java.util.Collections.singletonMap(Attribute.FIRE, 300))
                .build();
        TradeLine b = TradeLine.builder()
                .itemTemplateId(1L)
                .count(1L)
                .unitPrice(1L)
                .currencyItemTemplateId(57L)
                .attributes(java.util.Collections.singletonMap(Attribute.WATER, 300))
                .build();

        assertNotEquals(a, b);
    }
}
