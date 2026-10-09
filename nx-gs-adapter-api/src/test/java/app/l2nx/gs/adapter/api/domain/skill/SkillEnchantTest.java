package app.l2nx.gs.adapter.api.domain.skill;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SkillEnchantTest {

    @ParameterizedTest(name = "route={0}, level={1}")
    @CsvSource({"0,1", "-1,1", "1,0", "1,-1", "0,0"})
    void constructor_shouldReject_whenRouteOrLevelNotPositive(int route, int level) {
        assertThrows(IllegalArgumentException.class, () -> new SkillEnchant(route, level));
    }

    @Test
    void constructor_shouldExposeCoordinates() {
        SkillEnchant enchant = new SkillEnchant(2, 5);

        assertEquals(2, enchant.getRoute());
        assertEquals(5, enchant.getLevel());
    }

    @Test
    void equals_shouldCompareByRouteAndLevel() {
        assertEquals(new SkillEnchant(1, 2), new SkillEnchant(1, 2));
        assertEquals(new SkillEnchant(1, 2).hashCode(), new SkillEnchant(1, 2).hashCode());
        assertNotEquals(new SkillEnchant(1, 2), new SkillEnchant(2, 1));
        assertNotEquals(new SkillEnchant(1, 2), new SkillEnchant(1, 3));
    }
}
