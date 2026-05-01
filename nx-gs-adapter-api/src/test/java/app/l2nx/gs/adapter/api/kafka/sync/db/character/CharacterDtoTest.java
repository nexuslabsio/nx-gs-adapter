package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore;
import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CharacterDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        List<CharacterSubclassDto> subs = Arrays.asList(
                CharacterSubclassDto.builder().classId(CharacterClass.SOULTAKER).level(76).build(),
                CharacterSubclassDto.builder().classId(CharacterClass.HIEROPHANT).level(80).build());

        CharacterDto ch = CharacterDto.builder()
                .id(54321L)
                .name("Cliodhna")
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
                .build();

        assertEquals(54321L, ch.getId());
        assertEquals("Cliodhna", ch.getName());
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
    }

    @Test
    void allOptionalFields_shouldBeNullable_whenTenantOmitsThem() {
        CharacterDto ch = CharacterDto.builder().id(1L).build();

        assertEquals(1L, ch.getId());
        assertNull(ch.getName());
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
    }

    @Test
    void clanId_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDto ch = CharacterDto.builder().id(1L).clanId(null).build();

        assertNull(ch.getClanId());
    }

    @Test
    void subclasses_shouldBeNull_whenTenantDoesNotSyncThem() {
        CharacterDto ch = CharacterDto.builder().id(1L).build();

        assertNull(ch.getSubclasses());
    }

    @Test
    void subclasses_shouldBeEmptyList_whenTenantSyncsButCharHasNone() {
        CharacterDto ch = CharacterDto.builder()
                .id(1L)
                .subclasses(Collections.emptyList())
                .build();

        assertNotNull(ch.getSubclasses());
        assertTrue(ch.getSubclasses().isEmpty());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterDto fromBuilder = CharacterDto.builder().id(1L).build();
        CharacterDto fromCtor = new CharacterDto(1L, null, null, null, null, null,
                null, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<CharacterSubclassDto> subs = Collections.singletonList(
                CharacterSubclassDto.builder().classId(CharacterClass.SOULTAKER).level(76).build());
        CharacterDto original = new CharacterDto(1L, "X", "", 10,
                CharacterSex.MALE, CharacterRace.HUMAN,
                CharacterClass.HUMAN_FIGHTER, CharacterClass.HUMAN_FIGHTER,
                subs, CharacterPrivateStore.CRAFT,
                null, 0, 0, 0);

        assertEquals(original, original.toBuilder().build());
    }
}
