package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterClass;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CharacterSubclassDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        CharacterSubclassDto sub = CharacterSubclassDto.builder()
                .classId(CharacterClass.CARDINAL)
                .level(82)
                .build();

        assertEquals(CharacterClass.CARDINAL, sub.getClassId());
        assertEquals(82, sub.getLevel());
    }

    @Test
    void equals_shouldDistinguishOnFields() {
        CharacterSubclassDto a = CharacterSubclassDto.builder()
                .classId(CharacterClass.CARDINAL).level(82).build();
        CharacterSubclassDto b = CharacterSubclassDto.builder()
                .classId(CharacterClass.CARDINAL).level(82).build();
        CharacterSubclassDto c = CharacterSubclassDto.builder()
                .classId(CharacterClass.CARDINAL).level(83).build();
        CharacterSubclassDto d = CharacterSubclassDto.builder()
                .classId(CharacterClass.HIEROPHANT).level(82).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterSubclassDto original = new CharacterSubclassDto(CharacterClass.SOULTAKER, 76);

        assertEquals(original, original.toBuilder().build());
    }
}
