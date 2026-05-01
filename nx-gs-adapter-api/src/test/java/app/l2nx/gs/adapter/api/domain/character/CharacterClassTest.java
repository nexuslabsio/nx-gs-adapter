package app.l2nx.gs.adapter.api.domain.character;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CharacterClassTest {

    @Test
    void allClassIds_shouldBeUnique() {
        Set<Integer> seen = new HashSet<Integer>();
        for (CharacterClass c : CharacterClass.values()) {
            if (!seen.add(c.getId())) {
                fail("duplicate id " + c.getId() + " on " + c);
            }
        }
    }

    @Test
    void byId_shouldRoundTrip_forEveryConstant() {
        for (CharacterClass c : CharacterClass.values()) {
            assertSame(c, CharacterClass.byId(c.getId()),
                    "byId roundtrip failed for " + c);
        }
    }

    @Test
    void byId_shouldReturnNull_whenIdIsUnknown() {
        // Awakened-only classes, gaps in the canonical numbering, future chronicles.
        assertNull(CharacterClass.byId(58));
        assertNull(CharacterClass.byId(120));
        assertNull(CharacterClass.byId(196));
        assertNull(CharacterClass.byId(208));
        assertNull(CharacterClass.byId(254));
        assertNull(CharacterClass.byId(-1));
        assertNull(CharacterClass.byId(999));
    }

    @Test
    void canonicalSet_shouldHave103Entries() {
        assertEquals(103, CharacterClass.values().length);
    }

    @Test
    void canonicalSamples_shouldResolveToExpectedConstant() {
        assertSame(CharacterClass.HUMAN_FIGHTER, CharacterClass.byId(0));
        assertSame(CharacterClass.HUMAN_MAGE, CharacterClass.byId(10));
        assertSame(CharacterClass.WARLOCK, CharacterClass.byId(14));
        assertSame(CharacterClass.BISHOP, CharacterClass.byId(16));
        assertSame(CharacterClass.DUELIST, CharacterClass.byId(88));
        assertSame(CharacterClass.CARDINAL, CharacterClass.byId(97));
        assertSame(CharacterClass.KAMAEL_MALE_SOLDIER, CharacterClass.byId(123));
        assertSame(CharacterClass.JUDICATOR, CharacterClass.byId(136));
    }
}
