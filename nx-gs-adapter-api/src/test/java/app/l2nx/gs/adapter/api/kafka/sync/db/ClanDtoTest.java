package app.l2nx.gs.adapter.api.kafka.sync.db;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClanDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        List<ClanSkillDto> skills = Arrays.asList(
                ClanSkillDto.builder().skillId(101).skillLevel(3).build(),
                ClanSkillDto.builder().skillId(202).skillLevel(7).build());

        ClanDto clan = ClanDto.builder()
                .clanId(12345L)
                .clanName("Hellbound")
                .clanLevel(8)
                .leaderId(67890L)
                .allyId(42L)
                .skills(skills)
                .build();

        assertEquals(12345L, clan.getClanId());
        assertEquals("Hellbound", clan.getClanName());
        assertEquals(8, clan.getClanLevel());
        assertEquals(Long.valueOf(67890L), clan.getLeaderId());
        assertEquals(Long.valueOf(42L), clan.getAllyId());
        assertEquals(skills, clan.getSkills());
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
    void skills_shouldBeNull_whenTenantDoesNotSyncThem() {
        // Tenant whose schema provider does not declare a clan_skills child
        // source builds the DTO without calling .skills(...). Gson's default
        // serializeNulls=false omits the field from JSON, so the consumer
        // distinguishes "feature not synced" (null) from "feature synced,
        // empty list" (empty list).
        ClanDto clan = ClanDto.builder().clanId(1L).clanName("X").clanLevel(1).build();

        assertNull(clan.getSkills());
    }

    @Test
    void skills_shouldBeEmptyList_whenTenantSyncsThemButClanHasNone() {
        ClanDto clan = ClanDto.builder()
                .clanId(1L).clanName("X").clanLevel(1)
                .skills(Collections.emptyList())
                .build();

        assertNotNull(clan.getSkills());
        assertTrue(clan.getSkills().isEmpty());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<ClanSkillDto> skills = Collections.singletonList(
                ClanSkillDto.builder().skillId(1).skillLevel(2).build());
        ClanDto original = new ClanDto(1L, "X", 5, 2L, null, skills);

        assertEquals(original, original.toBuilder().build());
    }
}
