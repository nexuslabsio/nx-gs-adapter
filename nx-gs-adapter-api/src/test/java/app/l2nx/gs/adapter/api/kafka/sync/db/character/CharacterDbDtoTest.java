package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore;
import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CharacterDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        List<CharacterSubclassDbDto> subs = Arrays.asList(
                CharacterSubclassDbDto.builder().classId(CharacterClass.SOULTAKER).level(76).build(),
                CharacterSubclassDbDto.builder().classId(CharacterClass.HIEROPHANT).level(80).build());
        Instant deleteAt = Instant.parse("2026-06-01T12:00:00Z");

        CharacterDbDto ch = CharacterDbDto.builder()
                .id(54321L)
                .name("Cliodhna")
                .accountName("kiryl@nexus")
                .title("Hellbound")
                .level(85)
                .sex(CharacterSex.FEMALE)
                .race(CharacterRace.ELF)
                .classId(CharacterClass.EVA_SAINT)
                .baseClassId(CharacterClass.ELDER)
                .subclasses(subs)
                .privateStore(CharacterPrivateStore.SELL)
                .clanId(909L)
                .pvpCounter(1200)
                .pkCounter(7)
                .karma(0)
                .noblesse(Boolean.TRUE)
                .scheduledDeletionAt(deleteAt)
                .online(Boolean.TRUE)
                .onlineTimeSeconds(86_400L)
                .hero(Boolean.TRUE)
                .build();

        assertEquals(54321L, ch.getId());
        assertEquals("Cliodhna", ch.getName());
        assertEquals("kiryl@nexus", ch.getAccountName());
        assertEquals("Hellbound", ch.getTitle());
        assertEquals(Integer.valueOf(85), ch.getLevel());
        assertEquals(CharacterSex.FEMALE, ch.getSex());
        assertEquals(CharacterRace.ELF, ch.getRace());
        assertEquals(CharacterClass.EVA_SAINT, ch.getClassId());
        assertEquals(CharacterClass.ELDER, ch.getBaseClassId());
        assertEquals(subs, ch.getSubclasses());
        assertEquals(CharacterPrivateStore.SELL, ch.getPrivateStore());
        assertEquals(Long.valueOf(909L), ch.getClanId());
        assertEquals(Integer.valueOf(1200), ch.getPvpCounter());
        assertEquals(Integer.valueOf(7), ch.getPkCounter());
        assertEquals(Integer.valueOf(0), ch.getKarma());
        assertEquals(Boolean.TRUE, ch.getNoblesse());
        assertEquals(deleteAt, ch.getScheduledDeletionAt());
        assertEquals(Boolean.TRUE, ch.getOnline());
        assertEquals(Long.valueOf(86_400L), ch.getOnlineTimeSeconds());
        assertEquals(Boolean.TRUE, ch.getHero());
    }

    @Test
    void allOptionalFields_shouldBeNullable_whenTenantOmitsThemButNameRequired() {
        CharacterDbDto ch = CharacterDbDto.builder().id(1L).name("OnlyName").build();

        assertEquals(1L, ch.getId());
        assertEquals("OnlyName", ch.getName());
        assertNull(ch.getAccountName());
        assertNull(ch.getTitle());
        assertNull(ch.getLevel());
        assertNull(ch.getSex());
        assertNull(ch.getRace());
        assertNull(ch.getClassId());
        assertNull(ch.getBaseClassId());
        assertNull(ch.getSubclasses());
        assertNull(ch.getPrivateStore());
        assertNull(ch.getClanId());
        assertNull(ch.getPvpCounter());
        assertNull(ch.getPkCounter());
        assertNull(ch.getKarma());
        assertNull(ch.getNoblesse());
        assertNull(ch.getScheduledDeletionAt());
        assertNull(ch.getOnline());
        assertNull(ch.getOnlineTimeSeconds());
        assertNull(ch.getHero());
    }

    @Test
    void build_shouldThrowNpe_whenNameIsNull() {
        CharacterDbDto.Builder b = CharacterDbDto.builder().id(1L);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void clanId_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDbDto ch = CharacterDbDto.builder().id(1L).name("X").clanId(null).build();

        assertNull(ch.getClanId());
    }

    @Test
    void deleteTime_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDbDto ch = CharacterDbDto.builder().id(1L).name("X").scheduledDeletionAt(null).build();

        assertNull(ch.getScheduledDeletionAt());
    }

    @Test
    void subclasses_shouldBeNull_whenTenantDoesNotSyncThem() {
        CharacterDbDto ch = CharacterDbDto.builder().id(1L).name("X").build();

        assertNull(ch.getSubclasses());
    }

    @Test
    void subclasses_shouldBeEmptyList_whenTenantSyncsButCharHasNone() {
        CharacterDbDto ch = CharacterDbDto.builder()
                .id(1L)
                .name("X")
                .subclasses(Collections.emptyList())
                .build();

        assertNotNull(ch.getSubclasses());
        assertTrue(ch.getSubclasses().isEmpty());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterDbDto fromBuilder = CharacterDbDto.builder().id(1L).name("X").build();
        CharacterDbDto fromCtor = new CharacterDbDto(1L, "X", null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<CharacterSubclassDbDto> subs = Collections.singletonList(
                CharacterSubclassDbDto.builder().classId(CharacterClass.SOULTAKER).level(76).build());
        CharacterDbDto original = new CharacterDbDto(1L, "X", "acc", "", 10,
                CharacterSex.MALE, CharacterRace.HUMAN,
                CharacterClass.HUMAN_FIGHTER, CharacterClass.HUMAN_FIGHTER,
                subs, CharacterPrivateStore.CRAFT,
                null, 0, 0, 0, Boolean.FALSE, Instant.parse("2026-07-01T00:00:00Z"), Boolean.TRUE,
                86_400L, Boolean.TRUE);

        assertEquals(original, original.toBuilder().build());
    }
}
