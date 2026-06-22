package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClanDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        List<ClanSkillDbDto> skills = Arrays.asList(
                ClanSkillDbDto.builder().id(101).level(3).build(),
                ClanSkillDbDto.builder().id(202).level(7).build());

        ClanDbDto clan = ClanDbDto.builder()
                .id(12345L)
                .name("Hellbound")
                .level(8)
                .leaderId(67890L)
                .allyId(42L)
                .skills(skills)
                .build();

        assertEquals(12345L, clan.getId());
        assertEquals("Hellbound", clan.getName());
        assertEquals(Integer.valueOf(8), clan.getLevel());
        assertEquals(Long.valueOf(67890L), clan.getLeaderId());
        assertEquals(Long.valueOf(42L), clan.getAllyId());
        assertEquals(skills, clan.getSkills());
    }

    @Test
    void level_shouldBeNullable_whenTenantDoesNotSurfaceIt() {
        ClanDbDto clan = ClanDbDto.builder().id(1L).name("X").build();

        assertNull(clan.getLevel());
    }

    @Test
    void build_shouldThrowNpe_whenNameIsNull() {
        ClanDbDto.Builder b = ClanDbDto.builder().id(1L);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void leaderAndAlly_shouldBeNullable_forSentinelZeroSourceValue() {
        ClanDbDto clan = ClanDbDto.builder()
                .id(1L)
                .name("LonelyClan")
                .level(0)
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
        ClanDbDto clan = ClanDbDto.builder().id(1L).name("X").level(1).build();

        assertNull(clan.getSkills());
    }

    @Test
    void skills_shouldBeEmptyList_whenTenantSyncsThemButClanHasNone() {
        ClanDbDto clan = ClanDbDto.builder()
                .id(1L)
                .name("X")
                .level(1)
                .skills(Collections.emptyList())
                .build();

        assertNotNull(clan.getSkills());
        assertTrue(clan.getSkills().isEmpty());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<ClanSkillDbDto> skills = Collections.singletonList(
                ClanSkillDbDto.builder().id(1).level(2).build());
        ClanDbDto original = ClanDbDto.builder()
                .id(1L)
                .name("X")
                .level(5)
                .leaderId(2L)
                .skills(skills)
                .icon(new byte[] {1, 2, 3})
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void icon_shouldBeNull_whenTenantDoesNotSyncCrests() {
        ClanDbDto clan = ClanDbDto.builder().id(1L).name("X").level(1).build();
        assertNull(clan.getIcon());
    }

    @Test
    void equals_shouldCompareIconBytes() {
        ClanDbDto a = ClanDbDto.builder()
                .id(1L)
                .name("X")
                .level(1)
                .icon(new byte[] {1, 2})
                .build();
        ClanDbDto b = ClanDbDto.builder()
                .id(1L)
                .name("X")
                .level(1)
                .icon(new byte[] {1, 2})
                .build();
        ClanDbDto c = ClanDbDto.builder()
                .id(1L)
                .name("X")
                .level(1)
                .icon(new byte[] {1, 3})
                .build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
