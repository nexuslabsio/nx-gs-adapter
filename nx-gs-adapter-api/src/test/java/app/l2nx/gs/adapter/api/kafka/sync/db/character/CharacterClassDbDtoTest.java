package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassKind;
import org.junit.jupiter.api.Test;

class CharacterClassDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        CharacterClassDbDto entry = CharacterClassDbDto.builder()
                .classId(CharacterClass.CARDINAL)
                .kind(CharacterClassKind.SUB)
                .level(82)
                .exp(123_456L)
                .sp(789L)
                .build();

        assertEquals(CharacterClass.CARDINAL, entry.getClassId());
        assertEquals(CharacterClassKind.SUB, entry.getKind());
        assertEquals(Integer.valueOf(82), entry.getLevel());
        assertEquals(Long.valueOf(123_456L), entry.getExp());
        assertEquals(Long.valueOf(789L), entry.getSp());
    }

    @Test
    void levelExpSp_shouldBeNullable_whenTenantOmitsTheColumns() {
        CharacterClassDbDto entry = CharacterClassDbDto.builder()
                .classId(CharacterClass.CARDINAL)
                .kind(CharacterClassKind.MAIN)
                .build();

        assertNull(entry.getLevel());
        assertNull(entry.getExp());
        assertNull(entry.getSp());
    }

    @Test
    void build_shouldThrowNpe_whenClassIdIsNull() {
        CharacterClassDbDto.Builder b = CharacterClassDbDto.builder().kind(CharacterClassKind.MAIN);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void build_shouldThrowNpe_whenKindIsNull() {
        CharacterClassDbDto.Builder b = CharacterClassDbDto.builder().classId(CharacterClass.CARDINAL);

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void equals_shouldDistinguishOnFields() {
        CharacterClassDbDto a = CharacterClassDbDto.builder()
                .classId(CharacterClass.CARDINAL)
                .kind(CharacterClassKind.SUB)
                .level(82)
                .exp(100L)
                .sp(10L)
                .build();
        CharacterClassDbDto same = a.toBuilder().build();
        CharacterClassDbDto otherKind =
                a.toBuilder().kind(CharacterClassKind.MAIN).build();
        CharacterClassDbDto otherLevel = a.toBuilder().level(83).build();
        CharacterClassDbDto otherExp = a.toBuilder().exp(101L).build();
        CharacterClassDbDto otherSp = a.toBuilder().sp(11L).build();
        CharacterClassDbDto otherClass =
                a.toBuilder().classId(CharacterClass.HIEROPHANT).build();

        assertEquals(a, same);
        assertEquals(a.hashCode(), same.hashCode());
        assertNotEquals(a, otherKind);
        assertNotEquals(a, otherLevel);
        assertNotEquals(a, otherExp);
        assertNotEquals(a, otherSp);
        assertNotEquals(a, otherClass);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterClassDbDto original =
                new CharacterClassDbDto(CharacterClass.SOULTAKER, CharacterClassKind.SUB, 76, 5L, 6L);

        assertEquals(original, original.toBuilder().build());
    }
}
