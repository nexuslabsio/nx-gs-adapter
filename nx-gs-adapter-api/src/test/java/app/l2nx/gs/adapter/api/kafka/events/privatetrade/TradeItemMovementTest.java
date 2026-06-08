package app.l2nx.gs.adapter.api.kafka.events.privatetrade;

import app.l2nx.gs.adapter.api.domain.Attribute;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeItemMovementTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .attributes(null)
                .build();

        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .attributes(source)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> line.getAttributes().put(Attribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .attributes(source)
                .build();

        source.put(Attribute.WATER, 150);

        assertEquals(1, line.getAttributes().size());
        assertEquals(Integer.valueOf(300), line.getAttributes().get(Attribute.FIRE));
    }

    @Test
    void enchantLevel_shouldDefaultToNull_signalingNotApplicable() {
        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .build();

        assertNull(line.getEnchantLevel());
    }

    @Test
    void enchantLevel_shouldRoundtripExplicitZero() {
        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(1L).newItemId(2L)
                .count(1L)
                .enchantLevel(0)
                .build();

        assertEquals(Integer.valueOf(0), line.getEnchantLevel());
    }

    @Test
    void getNewItemId_shouldRoundtripDistinctFromItemId() {
        TradeItemMovement line = TradeItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(11L).newItemId(22L)
                .count(1L)
                .build();

        assertEquals(11L, line.getItemId());
        assertEquals(22L, line.getNewItemId());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<Attribute, Integer> attrs = new LinkedHashMap<Attribute, Integer>();
        attrs.put(Attribute.FIRE, 300);

        TradeItemMovement original = TradeItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(11L).newItemId(22L)
                .count(5L)
                .enchantLevel(16)
                .attributes(attrs)
                .build();

        TradeItemMovement copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishAttributes() {
        TradeItemMovement a = TradeItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(1L).count(1L)
                .attributes(Collections.singletonMap(Attribute.FIRE, 300))
                .build();
        TradeItemMovement b = TradeItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(1L).count(1L)
                .attributes(Collections.singletonMap(Attribute.WATER, 300))
                .build();

        assertNotEquals(a, b);
    }
}
