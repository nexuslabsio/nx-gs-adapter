package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BoughtLineTest {

    @Test
    void getEnchantLevel_shouldDefaultToNull() {
        BoughtLine line = BoughtLine.builder()
                .itemId(1)
                .itemTemplateId(57L)
                .count(1L)
                .unitPriceAdena(100L)
                .build();

        assertNull(line.getEnchantLevel());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        BoughtLine original = BoughtLine.builder()
                .itemId(268437521)
                .itemTemplateId(57L)
                .enchantLevel(16)
                .count(3L)
                .unitPriceAdena(1_500_000L)
                .build();

        BoughtLine copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishUnitPriceAdena() {
        BoughtLine a = BoughtLine.builder()
                .itemId(1)
                .itemTemplateId(57L)
                .count(1L)
                .unitPriceAdena(100L)
                .build();
        BoughtLine b = a.toBuilder().unitPriceAdena(101L).build();

        assertTrue(!a.equals(b));
    }
}
