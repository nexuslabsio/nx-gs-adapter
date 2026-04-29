package app.l2nx.gs.adapter.api.kafka.sync.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ClanDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        ClanDto clan = ClanDto.builder()
                .clanId(12345L)
                .clanName("Hellbound")
                .clanLevel(8)
                .leaderId(67890L)
                .allyId(42L)
                .build();

        assertEquals(12345L, clan.getClanId());
        assertEquals("Hellbound", clan.getClanName());
        assertEquals(8, clan.getClanLevel());
        assertEquals(Long.valueOf(67890L), clan.getLeaderId());
        assertEquals(Long.valueOf(42L), clan.getAllyId());
    }

    @Test
    void leaderAndAlly_shouldBeNullable_forSentinelZeroSourceValue() {
        ClanDto clan = ClanDto.builder()
                .clanId(1L)
                .clanName("LonelyClan")
                .clanLevel(0)
                .leaderId(null)
                .allyId(null)
                .build();

        assertNull(clan.getLeaderId());
        assertNull(clan.getAllyId());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ClanDto original = new ClanDto(1L, "X", 5, 2L, null);

        assertEquals(original, original.toBuilder().build());
    }
}
