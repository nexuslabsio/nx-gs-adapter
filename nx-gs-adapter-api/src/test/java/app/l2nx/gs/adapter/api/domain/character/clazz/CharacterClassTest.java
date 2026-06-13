package app.l2nx.gs.adapter.api.domain.character.clazz;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CharacterClassTest {

    @Test
    void canonicalSet_shouldHave103Entries() {
        assertEquals(103, CharacterClass.values().length);
    }

    @Test
    void canonicalSamples_shouldBePresent() {
        assertNotNull(CharacterClass.valueOf("HUMAN_FIGHTER"));
        assertNotNull(CharacterClass.valueOf("DUELIST"));
        assertNotNull(CharacterClass.valueOf("KAMAEL_MALE_SOLDIER"));
        assertNotNull(CharacterClass.valueOf("JUDICATOR"));
    }
}
