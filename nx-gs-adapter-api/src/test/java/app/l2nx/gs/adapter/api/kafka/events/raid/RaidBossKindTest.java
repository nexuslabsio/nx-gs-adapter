package app.l2nx.gs.adapter.api.kafka.events.raid;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RaidBossKindTest {

    @Test
    void values_shouldExposeThreeConstants() {
        assertArrayEquals(
                new RaidBossKind[] {RaidBossKind.RAID, RaidBossKind.EPIC, RaidBossKind.INSTANCE_BOSS},
                RaidBossKind.values());
    }

    @Test
    void valueOf_shouldRoundtripAllConstants() {
        assertEquals(RaidBossKind.RAID, RaidBossKind.valueOf("RAID"));
        assertEquals(RaidBossKind.EPIC, RaidBossKind.valueOf("EPIC"));
        assertEquals(RaidBossKind.INSTANCE_BOSS, RaidBossKind.valueOf("INSTANCE_BOSS"));
    }

    @Test
    void name_shouldMatchEnumLiteral() {
        // Wire reflects the enum literal (Gson default) — pin the format so a
        // refactor renaming the constants surfaces here, not on the platform side.
        assertEquals("RAID", RaidBossKind.RAID.name());
        assertEquals("EPIC", RaidBossKind.EPIC.name());
        assertEquals("INSTANCE_BOSS", RaidBossKind.INSTANCE_BOSS.name());
    }
}
