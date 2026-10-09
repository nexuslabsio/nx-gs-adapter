package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import app.l2nx.gs.adapter.api.domain.skill.SkillEffectCategory;
import app.l2nx.gs.adapter.api.domain.skill.SkillEnchant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public final class CharacterEffect {

    private final int skillId;
    private final int skillLevel;
    private final SkillEffectCategory category;
    private final @Nullable Integer remainingSec;
    private final CharacterEffectOffline offline;
    private final @Nullable SkillEnchant enchant;

    public CharacterEffect(
            int skillId,
            int skillLevel,
            SkillEffectCategory category,
            @Nullable Integer remainingSec,
            CharacterEffectOffline offline,
            @Nullable SkillEnchant enchant) {
        this.skillId = skillId;
        this.skillLevel = skillLevel;
        this.category = Objects.requireNonNull(category, "category");
        this.remainingSec = remainingSec;
        this.offline = Objects.requireNonNull(offline, "offline");
        this.enchant = enchant;
    }

    public int getSkillId() {
        return skillId;
    }

    /** Together with {@code skillId} and {@code enchant} keys one gd-sync skill record. */
    public int getSkillLevel() {
        return skillLevel;
    }

    /** Enchant step of the record; null = the skill is not enchanted. */
    public @Nullable SkillEnchant getEnchant() {
        return enchant;
    }

    public SkillEffectCategory getCategory() {
        return category;
    }

    /**
     * Whole seconds left at snapshot time; null when there is no duration. Left out of the host change hash, so
     * consumers count it down from the sync moment.
     */
    public @Nullable Integer getRemainingSec() {
        return remainingSec;
    }

    public CharacterEffectOffline getOffline() {
        return offline;
    }

    public Builder toBuilder() {
        return new Builder()
                .skillId(skillId)
                .skillLevel(skillLevel)
                .category(category)
                .remainingSec(remainingSec)
                .offline(offline)
                .enchant(enchant);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterEffect)) return false;
        CharacterEffect that = (CharacterEffect) o;
        return skillId == that.skillId
                && skillLevel == that.skillLevel
                && category == that.category
                && Objects.equals(remainingSec, that.remainingSec)
                && offline == that.offline
                && Objects.equals(enchant, that.enchant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(skillId, skillLevel, category, remainingSec, offline, enchant);
    }

    @Override
    public String toString() {
        return "CharacterEffect[skillId=" + skillId
                + ", skillLevel=" + skillLevel
                + ", category=" + category
                + ", remainingSec=" + remainingSec
                + ", offline=" + offline
                + ", enchant=" + enchant + "]";
    }

    public static final class Builder {
        private int skillId;
        private int skillLevel;
        private @Nullable SkillEffectCategory category;
        private @Nullable Integer remainingSec;
        private @Nullable CharacterEffectOffline offline;
        private @Nullable SkillEnchant enchant;

        public Builder skillId(int skillId) {
            this.skillId = skillId;
            return this;
        }

        public Builder skillLevel(int skillLevel) {
            this.skillLevel = skillLevel;
            return this;
        }

        public Builder category(SkillEffectCategory category) {
            this.category = category;
            return this;
        }

        public Builder remainingSec(@Nullable Integer remainingSec) {
            this.remainingSec = remainingSec;
            return this;
        }

        public Builder offline(CharacterEffectOffline offline) {
            this.offline = offline;
            return this;
        }

        public Builder enchant(@Nullable SkillEnchant enchant) {
            this.enchant = enchant;
            return this;
        }

        public CharacterEffect build() {
            return new CharacterEffect(skillId, skillLevel, category, remainingSec, offline, enchant);
        }
    }
}
