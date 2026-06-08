package app.l2nx.gs.adapter.api.kafka.events.mail;

import app.l2nx.gs.adapter.api.domain.Attribute;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MailItemMovementTest {

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .build();

        assertTrue(movement.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .attributes(null)
                .build();

        assertTrue(movement.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .attributes(source)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> movement.getAttributes().put(Attribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<Attribute, Integer> source = new HashMap<Attribute, Integer>();
        source.put(Attribute.FIRE, 300);

        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .attributes(source)
                .build();

        source.put(Attribute.WATER, 150);

        assertEquals(1, movement.getAttributes().size());
        assertEquals(Integer.valueOf(300), movement.getAttributes().get(Attribute.FIRE));
    }

    @Test
    void enchantLevel_shouldDefaultToNull_signalingNotApplicable() {
        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .build();

        assertNull(movement.getEnchantLevel());
    }

    @Test
    void enchantLevel_shouldRoundtripExplicitZero() {
        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L).newItemId(268437521L)
                .count(1L)
                .enchantLevel(0)
                .build();

        assertEquals(Integer.valueOf(0), movement.getEnchantLevel());
    }

    @Test
    void getNewItemId_shouldRoundtripDistinctFromItemId() {
        MailItemMovement movement = MailItemMovement.builder()
                .itemTemplateId(57L)
                .itemId(268437521L)
                .newItemId(268437522L)
                .count(1L)
                .build();

        assertEquals(268437521L, movement.getItemId());
        assertEquals(268437522L, movement.getNewItemId());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<Attribute, Integer> attrs = new LinkedHashMap<Attribute, Integer>();
        attrs.put(Attribute.FIRE, 300);

        MailItemMovement original = MailItemMovement.builder()
                .itemTemplateId(6611L)
                .itemId(268437521L)
                .newItemId(268437522L)
                .count(5L)
                .enchantLevel(16)
                .attributes(attrs)
                .build();

        MailItemMovement copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishNewItemId() {
        MailItemMovement a = MailItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(2L).count(1L).build();
        MailItemMovement b = MailItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(3L).count(1L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishAttributes() {
        MailItemMovement a = MailItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(1L).count(1L)
                .attributes(Collections.singletonMap(Attribute.FIRE, 300))
                .build();
        MailItemMovement b = MailItemMovement.builder()
                .itemTemplateId(57L).itemId(1L).newItemId(1L).count(1L)
                .attributes(Collections.singletonMap(Attribute.WATER, 300))
                .build();

        assertNotEquals(a, b);
    }
}
