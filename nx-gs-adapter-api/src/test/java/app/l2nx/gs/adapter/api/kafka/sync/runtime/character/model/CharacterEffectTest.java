package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.skill.SkillEffectCategory;
import app.l2nx.gs.adapter.api.domain.skill.SkillEnchant;
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
    void toBuilder_shouldKeepEnchant() {
        CharacterEffect original = haste().enchant(new SkillEnchant(2, 3)).build();

        assertEquals(new SkillEnchant(2, 3), original.toBuilder().build().getEnchant());
        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void build_shouldAcceptNullEnchant_forNotEnchantedSkills() {
        assertNull(haste().build().getEnchant());
    }

    @Test
    void equals_shouldDiffer_whenEnchantDiffers() {
        CharacterEffect plain = haste().build();
        CharacterEffect enchanted = haste().enchant(new SkillEnchant(1, 1)).build();

        assertNotEquals(plain, enchanted);
        assertNotEquals(enchanted, haste().enchant(new SkillEnchant(1, 2)).build());
        assertNotEquals(enchanted, haste().enchant(new SkillEnchant(2, 1)).build());
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
