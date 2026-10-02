package app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * {@code kind} ({@code BASE}/{@code SHIELD}/{@code ENCHANT6}/{@code ENCHANT_BY}) says when the skill applies. {@code minPieces}
 * qualifies {@code BASE} ({@code null} = full set); {@code enchantLevel} qualifies {@code ENCHANT_BY}, which may repeat per level.
 */
public final class ArmorSetSkill {

    private final int skillTemplateId;
    private final @Nullable Integer skillLevel;
    private final String kind;
    private final @Nullable Integer minPieces;
    private final @Nullable Integer enchantLevel;

    public ArmorSetSkill(
            int skillTemplateId,
            @Nullable Integer skillLevel,
            String kind,
            @Nullable Integer minPieces,
            @Nullable Integer enchantLevel) {
        this.skillTemplateId = skillTemplateId;
        this.skillLevel = skillLevel;
        this.kind = Objects.requireNonNull(kind, "kind");
        this.minPieces = minPieces;
        this.enchantLevel = enchantLevel;
    }

    public int getSkillTemplateId() {
        return skillTemplateId;
    }

    public @Nullable Integer getSkillLevel() {
        return skillLevel;
    }

    public String getKind() {
        return kind;
    }

    public @Nullable Integer getMinPieces() {
        return minPieces;
    }

    public @Nullable Integer getEnchantLevel() {
        return enchantLevel;
    }

    public Builder toBuilder() {
        return new Builder()
                .skillTemplateId(skillTemplateId)
                .skillLevel(skillLevel)
                .kind(kind)
                .minPieces(minPieces)
                .enchantLevel(enchantLevel);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArmorSetSkill)) return false;
        ArmorSetSkill that = (ArmorSetSkill) o;
        return skillTemplateId == that.skillTemplateId
                && Objects.equals(skillLevel, that.skillLevel)
                && Objects.equals(kind, that.kind)
                && Objects.equals(minPieces, that.minPieces)
                && Objects.equals(enchantLevel, that.enchantLevel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(skillTemplateId, skillLevel, kind, minPieces, enchantLevel);
    }

    @Override
    public String toString() {
        return "ArmorSetSkill[skillTemplateId=" + skillTemplateId + ", skillLevel=" + skillLevel + ", kind=" + kind
                + "]";
    }

    public static final class Builder {
        private int skillTemplateId;
        private @Nullable Integer skillLevel;
        private String kind;
        private @Nullable Integer minPieces;
        private @Nullable Integer enchantLevel;

        public Builder skillTemplateId(int skillTemplateId) {
            this.skillTemplateId = skillTemplateId;
            return this;
        }

        public Builder skillLevel(@Nullable Integer skillLevel) {
            this.skillLevel = skillLevel;
            return this;
        }

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder minPieces(@Nullable Integer minPieces) {
            this.minPieces = minPieces;
            return this;
        }

        public Builder enchantLevel(@Nullable Integer enchantLevel) {
            this.enchantLevel = enchantLevel;
            return this;
        }

        public ArmorSetSkill build() {
            return new ArmorSetSkill(skillTemplateId, skillLevel, kind, minPieces, enchantLevel);
        }
    }
}
