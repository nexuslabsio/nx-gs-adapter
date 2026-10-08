package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.skill.SkillEffectCategory;
import org.junit.jupiter.api.Test;

class CharacterEffectTest {

    private static CharacterEffect.Builder haste() {
        return CharacterEffect.builder()
                .skillId(1086)
                .skillLevel(2)
                .category(SkillEffectCategory.BUFF)
                .remainingSec(1180)
                .offline(CharacterEffectOffline.FROZEN);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterEffect original = haste().build();

        assertEquals(original, original.toBuilder().build());
        assertEquals(original.hashCode(), original.toBuilder().build().hashCode());
    }

    @Test
    void build_shouldAcceptNullRemainingSec_forEffectsWithoutDuration() {
        assertNull(haste().category(SkillEffectCategory.TOGGLE)
                .remainingSec(null)
                .build()
                .getRemainingSec());
    }

    @Test
    void build_shouldReject_whenCategoryMissing() {
        assertThrows(NullPointerException.class, () -> haste().category(null).build());
    }

    @Test
    void build_shouldReject_whenOfflineMissing() {
        assertThrows(NullPointerException.class, () -> haste().offline(null).build());
    }
}
