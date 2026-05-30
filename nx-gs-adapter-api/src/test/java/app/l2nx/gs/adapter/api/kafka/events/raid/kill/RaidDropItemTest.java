package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaidDropItemTest {

    @Test
    void getEnchantLevel_shouldBeNullable() {
        RaidDropItem drop = RaidDropItem.builder()
                .itemId(57)
                .count(100_000_000L)
                .build();

        assertNull(drop.getEnchantLevel());
    }

    @Test
    void builder_shouldPopulateAllFields() {
        RaidDropItem drop = RaidDropItem.builder()
                .itemId(6660)
                .count(1L)
                .enchantLevel(5)
                .build();

        assertEquals(6660, drop.getItemId());
        assertEquals(1L, drop.getCount());
        assertEquals(Integer.valueOf(5), drop.getEnchantLevel());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        RaidDropItem original = RaidDropItem.builder()
                .itemId(6660)
                .count(1L)
                .enchantLevel(8)
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishEnchantLevel() {
        RaidDropItem a = RaidDropItem.builder().itemId(1).count(1L).enchantLevel(0).build();
        RaidDropItem b = RaidDropItem.builder().itemId(1).count(1L).enchantLevel(1).build();

        assertNotEquals(a, b);
    }
}
