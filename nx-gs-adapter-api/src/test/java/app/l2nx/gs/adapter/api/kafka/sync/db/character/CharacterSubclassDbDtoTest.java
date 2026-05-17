package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterClass;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CharacterSubclassDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        CharacterSubclassDbDto sub = CharacterSubclassDbDto.builder()
                .classId(CharacterClass.CARDINAL)
                .level(82)
                .build();

        assertEquals(CharacterClass.CARDINAL, sub.getClassId());
        assertEquals(82, sub.getLevel());
    }

    @Test
    void equals_shouldDistinguishOnFields() {
        CharacterSubclassDbDto a = CharacterSubclassDbDto.builder()
                .classId(CharacterClass.CARDINAL).level(82).build();
        CharacterSubclassDbDto b = CharacterSubclassDbDto.builder()
                .classId(CharacterClass.CARDINAL).level(82).build();
        CharacterSubclassDbDto c = CharacterSubclassDbDto.builder()
                .classId(CharacterClass.CARDINAL).level(83).build();
        CharacterSubclassDbDto d = CharacterSubclassDbDto.builder()
                .classId(CharacterClass.HIEROPHANT).level(82).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterSubclassDbDto original = new CharacterSubclassDbDto(CharacterClass.SOULTAKER, 76);

        assertEquals(original, original.toBuilder().build());
    }
}
