package app.l2nx.gs.adapter.api.kafka.events.raid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RaidBossKindTest {

    @Test
    void values_shouldExposeThreeConstants() {
        assertArrayEquals(
                new RaidBossKind[]{RaidBossKind.RAID, RaidBossKind.GRAND_BOSS, RaidBossKind.INSTANCE_BOSS},
                RaidBossKind.values());
    }

    @Test
    void valueOf_shouldRoundtripAllConstants() {
        assertEquals(RaidBossKind.RAID, RaidBossKind.valueOf("RAID"));
        assertEquals(RaidBossKind.GRAND_BOSS, RaidBossKind.valueOf("GRAND_BOSS"));
        assertEquals(RaidBossKind.INSTANCE_BOSS, RaidBossKind.valueOf("INSTANCE_BOSS"));
    }

    @Test
    void name_shouldMatchEnumLiteral() {
        // Wire reflects the enum literal (Gson default) — pin the format so a
        // refactor renaming the constants surfaces here, not on the platform side.
        assertEquals("RAID", RaidBossKind.RAID.name());
        assertEquals("GRAND_BOSS", RaidBossKind.GRAND_BOSS.name());
        assertEquals("INSTANCE_BOSS", RaidBossKind.INSTANCE_BOSS.name());
    }
}
