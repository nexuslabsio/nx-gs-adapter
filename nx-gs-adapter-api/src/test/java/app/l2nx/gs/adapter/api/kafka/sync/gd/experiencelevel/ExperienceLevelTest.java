package app.l2nx.gs.adapter.api.kafka.sync.gd.experiencelevel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class ExperienceLevelTest {

    @Test
    void builder_shouldCarryLevelAndRequiredExp_beyondIntRange() {
        ExperienceLevel e =
                ExperienceLevel.builder().level(80).requiredExp(4_174_867_851L).build();

        assertEquals(80, e.getLevel());
        assertEquals(4_174_867_851L, e.getRequiredExp());
    }

    @Test
    void toBuilder_shouldProduceEqualCopy() {
        ExperienceLevel original =
                ExperienceLevel.builder().level(40).requiredExp(6_733_999L).build();

        ExperienceLevel copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
    }

    @Test
    void equals_shouldDiffer_whenRequiredExpDiffers() {
        assertNotEquals(
                ExperienceLevel.builder().level(1).requiredExp(0L).build(),
                ExperienceLevel.builder().level(1).requiredExp(1L).build());
    }
}
