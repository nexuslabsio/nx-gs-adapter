package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassKind;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CharacterDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
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
                .clanId(909L)
                .pvpCounter(1200)
                .pkCounter(7)
                .karma(0)
                .noblesse(Boolean.TRUE)
                .scheduledDeletionAt(deleteAt)
                .online(Boolean.TRUE)
                .onlineTimeSeconds(86_400L)
                .hero(Boolean.TRUE)
                .accessLevel("7")
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
        assertEquals(Long.valueOf(909L), ch.getClanId());
        assertEquals(Integer.valueOf(1200), ch.getPvpCounter());
        assertEquals(Integer.valueOf(7), ch.getPkCounter());
        assertEquals(Integer.valueOf(0), ch.getKarma());
        assertEquals(Boolean.TRUE, ch.getNoblesse());
        assertEquals(deleteAt, ch.getScheduledDeletionAt());
        assertEquals(Boolean.TRUE, ch.getOnline());
        assertEquals(Long.valueOf(86_400L), ch.getOnlineTimeSeconds());
        assertEquals(Boolean.TRUE, ch.getHero());
        assertEquals("7", ch.getAccessLevel());
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
        assertNull(ch.getClanId());
        assertNull(ch.getPvpCounter());
        assertNull(ch.getPkCounter());
        assertNull(ch.getKarma());
        assertNull(ch.getNoblesse());
        assertNull(ch.getScheduledDeletionAt());
        assertNull(ch.getOnline());
        assertNull(ch.getOnlineTimeSeconds());
        assertNull(ch.getHero());
        assertNull(ch.getAccessLevel());
        assertNull(ch.getLocks());
    }

    @Test
    void build_shouldThrowNpe_whenNameIsNull() {
        CharacterDbDto.Builder b = CharacterDbDto.builder().id(1L);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void clanId_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDbDto ch =
                CharacterDbDto.builder().id(1L).name("X").clanId(null).build();

        assertNull(ch.getClanId());
    }

    @Test
    void deleteTime_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDbDto ch = CharacterDbDto.builder()
                .id(1L)
                .name("X")
                .scheduledDeletionAt(null)
                .build();

        assertNull(ch.getScheduledDeletionAt());
    }

    @Test
    void classes_shouldBeNull_whenTenantDoesNotSyncThem() {
        CharacterDbDto ch = CharacterDbDto.builder().id(1L).name("X").build();

        assertNull(ch.getClasses());
    }

    @Test
    void classes_shouldCarryWholeRoster_includingMainClass() {
        CharacterDbDto ch = CharacterDbDto.builder()
                .id(1L)
                .name("X")
                .classId(CharacterClass.SOULTAKER)
                .baseClassId(CharacterClass.HUMAN_FIGHTER)
                .classes(Arrays.asList(
                        CharacterClassDbDto.builder()
                                .classId(CharacterClass.HUMAN_FIGHTER)
                                .kind(CharacterClassKind.MAIN)
                                .level(85)
                                .exp(9_000L)
                                .sp(70L)
                                .build(),
                        CharacterClassDbDto.builder()
                                .classId(CharacterClass.SOULTAKER)
                                .kind(CharacterClassKind.SUB)
                                .level(76)
                                .build()))
                .build();

        assertNotNull(ch.getClasses());
        assertEquals(2, ch.getClasses().size());
        assertEquals(CharacterClassKind.MAIN, ch.getClasses().get(0).getKind());
        assertEquals(CharacterClassKind.SUB, ch.getClasses().get(1).getKind());
        // The played class rides classId, never a flag on the roster entry.
        assertEquals(CharacterClass.SOULTAKER, ch.getClassId());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterDbDto fromBuilder = CharacterDbDto.builder().id(1L).name("X").build();
        CharacterDbDto fromCtor = new CharacterDbDto(
                1L, "X", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<CharacterClassDbDto> classes = Arrays.asList(
                CharacterClassDbDto.builder()
                        .classId(CharacterClass.HUMAN_FIGHTER)
                        .kind(CharacterClassKind.MAIN)
                        .level(10)
                        .exp(1234L)
                        .sp(56L)
                        .build(),
                CharacterClassDbDto.builder()
                        .classId(CharacterClass.SOULTAKER)
                        .kind(CharacterClassKind.SUB)
                        .level(76)
                        .build());
        List<CharacterInstanceCooldownDbDto> cooldowns =
                Collections.singletonList(CharacterInstanceCooldownDbDto.builder()
                        .instanceId(42)
                        .reentryAt(Instant.parse("2026-07-02T00:00:00Z"))
                        .build());
        List<CharacterLockDbDto> locks = Collections.singletonList(CharacterLockDbDto.builder()
                .lockType(WellKnownCharacterLockTypes.IP)
                .lockValue("127.0.0.1")
                .build());
        CharacterDbDto original = new CharacterDbDto(
                1L,
                "X",
                "acc",
                "",
                10,
                CharacterSex.MALE,
                CharacterRace.HUMAN,
                CharacterClass.HUMAN_FIGHTER,
                CharacterClass.HUMAN_FIGHTER,
                classes,
                null,
                0,
                0,
                0,
                Boolean.FALSE,
                Instant.parse("2026-07-01T00:00:00Z"),
                Boolean.TRUE,
                86_400L,
                Boolean.TRUE,
                Boolean.TRUE,
                1500,
                98_765L,
                "7",
                cooldowns,
                locks);

        assertEquals(original, original.toBuilder().build());
    }
}
