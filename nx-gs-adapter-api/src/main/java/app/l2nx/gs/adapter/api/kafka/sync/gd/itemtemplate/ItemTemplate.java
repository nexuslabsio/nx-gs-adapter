package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import app.l2nx.gs.adapter.api.domain.item.ItemClass;
import app.l2nx.gs.adapter.api.domain.item.ItemEquipSlot;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Build-agnostic item-template wire DTO — the common L2 denominator for static
 * item data, carried as the payload of {@code GameDataSyncEvent} on the {@code gd}
 * (game-data) sync stream's {@code item-templates} entity topic. Each host build
 * supplies its own provider that maps its core's internal item representation into
 * this shape; nothing here names a specific core.
 *
 * <p><b>Nullability:</b> only {@link #getId()} and {@link #getType()} are non-null.
 * Every other field is {@link Nullable} (former primitives boxed) so {@code null}
 * means "this build did not supply it" rather than a fabricated default. For the
 * boolean flags this yields an honest tri-state (true / false / unknown).</p>
 *
 * <p><b>Functional grouping:</b> cohesive areas are nested objects rather than a
 * flat scatter — {@link #getStats()} (combat numbers), {@link #getRestrictions()}
 * (trade / storage / permission flags), {@link #getUpgrade()} (enchant / attribute
 * / crystallize mechanics). A group is {@code null} when the item has no profile
 * for it (e.g. etc-items carry no {@code stats}).</p>
 *
 * <p><b>Vocabulary:</b> {@link #getEquipSlot()} is the closed {@link ItemEquipSlot} domain
 * enum. The open-string vocabulary fields ({@code material}, {@code grade},
 * {@code weaponType}, {@code armorType}, {@code etcItemType}, {@code defaultAction},
 * {@code useHandler}) draw from a platform-canonical vocabulary in {@code UPPER_SNAKE_CASE};
 * the provider translates its core's internal enums/bitmasks into it.</p>
 *
 * <p>Phase 1 is sourced from the host's already-parsed in-memory templates only —
 * client-patch fields (description, additional name, colour, panel icon, and the
 * patch-only permission flags) are a later slice and intentionally absent here.</p>
 */
public final class ItemTemplate {

    private final int id;
    private final ItemClass type;
    private final @Nullable String icon;
    private final @Nullable LocalizedText name;
    private final @Nullable Integer displayId;
    private final @Nullable Integer weight;
    private final @Nullable Long referencePrice;
    private final @Nullable String material;
    private final @Nullable String grade;
    private final @Nullable ItemEquipSlot equipSlot;
    private final @Nullable String weaponType;
    private final @Nullable String armorType;
    private final @Nullable String etcItemType;
    private final @Nullable Boolean stackable;
    private final @Nullable Boolean questItem;
    private final @Nullable Boolean petUsable;
    private final @Nullable String defaultAction;
    private final @Nullable String useHandler;
    private final @Nullable Integer duration;
    private final @Nullable Integer reuseDelay;
    private final @Nullable List<ItemSkillRef> skills;
    private final @Nullable ItemStats stats;
    private final @Nullable ItemRestrictions restrictions;
    private final @Nullable ItemUpgrade upgrade;

    private ItemTemplate(Builder b) {
        this.id = b.id;
        this.type = Objects.requireNonNull(b.type, "type");
        this.icon = b.icon;
        this.name = b.name;
        this.displayId = b.displayId;
        this.weight = b.weight;
        this.referencePrice = b.referencePrice;
        this.material = b.material;
        this.grade = b.grade;
        this.equipSlot = b.equipSlot;
        this.weaponType = b.weaponType;
        this.armorType = b.armorType;
        this.etcItemType = b.etcItemType;
        this.stackable = b.stackable;
        this.questItem = b.questItem;
        this.petUsable = b.petUsable;
        this.defaultAction = b.defaultAction;
        this.useHandler = b.useHandler;
        this.duration = b.duration;
        this.reuseDelay = b.reuseDelay;
        this.skills = b.skills == null ? null
                : Collections.unmodifiableList(new ArrayList<ItemSkillRef>(b.skills));
        this.stats = b.stats;
        this.restrictions = b.restrictions;
        this.upgrade = b.upgrade;
    }

    public int getId() {
        return id;
    }

    public ItemClass getType() {
        return type;
    }

    public @Nullable String getIcon() {
        return icon;
    }

    public @Nullable LocalizedText getName() {
        return name;
    }

    /**
     * Visual-override id (item renders as another); {@code null} = renders as itself.
     */
    public @Nullable Integer getDisplayId() {
        return displayId;
    }

    public @Nullable Integer getWeight() {
        return weight;
    }

    public @Nullable Long getReferencePrice() {
        return referencePrice;
    }

    public @Nullable String getMaterial() {
        return material;
    }

    public @Nullable String getGrade() {
        return grade;
    }

    public @Nullable ItemEquipSlot getEquipSlot() {
        return equipSlot;
    }

    public @Nullable String getWeaponType() {
        return weaponType;
    }

    public @Nullable String getArmorType() {
        return armorType;
    }

    public @Nullable String getEtcItemType() {
        return etcItemType;
    }

    public @Nullable Boolean getStackable() {
        return stackable;
    }

    public @Nullable Boolean getQuestItem() {
        return questItem;
    }

    /**
     * Whether a pet / summon can use this item.
     */
    public @Nullable Boolean getPetUsable() {
        return petUsable;
    }

    /**
     * Action on use (equip / use / soulshot / …).
     */
    public @Nullable String getDefaultAction() {
        return defaultAction;
    }

    /**
     * Use-effect handler name (etc-items only; {@code null} otherwise).
     */
    public @Nullable String getUseHandler() {
        return useHandler;
    }

    public @Nullable Integer getDuration() {
        return duration;
    }

    public @Nullable Integer getReuseDelay() {
        return reuseDelay;
    }

    /**
     * Skills the item grants ({@code skillId}+{@code level} refs); {@code null} if none.
     */
    public @Nullable List<ItemSkillRef> getSkills() {
        return skills;
    }

    /**
     * Combat stats (weapons / armor); {@code null} for items with no combat profile.
     */
    public @Nullable ItemStats getStats() {
        return stats;
    }

    /**
     * Trade / storage / permission flags; {@code null} if the build supplied none.
     */
    public @Nullable ItemRestrictions getRestrictions() {
        return restrictions;
    }

    /**
     * Enchant / attribute / crystallize mechanics; {@code null} if none apply.
     */
    public @Nullable ItemUpgrade getUpgrade() {
        return upgrade;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .type(type)
                .icon(icon)
                .name(name)
                .displayId(displayId)
                .weight(weight)
                .referencePrice(referencePrice)
                .material(material)
                .grade(grade)
                .equipSlot(equipSlot)
                .weaponType(weaponType)
                .armorType(armorType)
                .etcItemType(etcItemType)
                .stackable(stackable)
                .questItem(questItem)
                .petUsable(petUsable)
                .defaultAction(defaultAction)
                .useHandler(useHandler)
                .duration(duration)
                .reuseDelay(reuseDelay)
                .skills(skills)
                .stats(stats)
                .restrictions(restrictions)
                .upgrade(upgrade);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemTemplate)) return false;
        ItemTemplate that = (ItemTemplate) o;
        return id == that.id
                && type == that.type
                && Objects.equals(icon, that.icon)
                && Objects.equals(name, that.name)
                && Objects.equals(displayId, that.displayId)
                && Objects.equals(weight, that.weight)
                && Objects.equals(referencePrice, that.referencePrice)
                && Objects.equals(material, that.material)
                && Objects.equals(grade, that.grade)
                && Objects.equals(equipSlot, that.equipSlot)
                && Objects.equals(weaponType, that.weaponType)
                && Objects.equals(armorType, that.armorType)
                && Objects.equals(etcItemType, that.etcItemType)
                && Objects.equals(stackable, that.stackable)
                && Objects.equals(questItem, that.questItem)
                && Objects.equals(petUsable, that.petUsable)
                && Objects.equals(defaultAction, that.defaultAction)
                && Objects.equals(useHandler, that.useHandler)
                && Objects.equals(duration, that.duration)
                && Objects.equals(reuseDelay, that.reuseDelay)
                && Objects.equals(skills, that.skills)
                && Objects.equals(stats, that.stats)
                && Objects.equals(restrictions, that.restrictions)
                && Objects.equals(upgrade, that.upgrade);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, icon, name, displayId, weight, referencePrice, material,
                grade, equipSlot, weaponType, armorType, etcItemType, stackable, questItem,
                petUsable, defaultAction, useHandler, duration, reuseDelay, skills, stats,
                restrictions, upgrade);
    }

    @Override
    public String toString() {
        return "ItemTemplate[id=" + id + ", type=" + type + ", grade=" + grade + "]";
    }

    public static final class Builder {
        private int id;
        private ItemClass type;
        private @Nullable String icon;
        private @Nullable LocalizedText name;
        private @Nullable Integer displayId;
        private @Nullable Integer weight;
        private @Nullable Long referencePrice;
        private @Nullable String material;
        private @Nullable String grade;
        private @Nullable ItemEquipSlot equipSlot;
        private @Nullable String weaponType;
        private @Nullable String armorType;
        private @Nullable String etcItemType;
        private @Nullable Boolean stackable;
        private @Nullable Boolean questItem;
        private @Nullable Boolean petUsable;
        private @Nullable String defaultAction;
        private @Nullable String useHandler;
        private @Nullable Integer duration;
        private @Nullable Integer reuseDelay;
        private @Nullable List<ItemSkillRef> skills;
        private @Nullable ItemStats stats;
        private @Nullable ItemRestrictions restrictions;
        private @Nullable ItemUpgrade upgrade;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder type(ItemClass type) {
            this.type = type;
            return this;
        }

        public Builder icon(@Nullable String icon) {
            this.icon = icon;
            return this;
        }

        public Builder name(@Nullable LocalizedText name) {
            this.name = name;
            return this;
        }

        public Builder displayId(@Nullable Integer displayId) {
            this.displayId = displayId;
            return this;
        }

        public Builder weight(@Nullable Integer weight) {
            this.weight = weight;
            return this;
        }

        public Builder referencePrice(@Nullable Long referencePrice) {
            this.referencePrice = referencePrice;
            return this;
        }

        public Builder material(@Nullable String material) {
            this.material = material;
            return this;
        }

        public Builder grade(@Nullable String grade) {
            this.grade = grade;
            return this;
        }

        public Builder equipSlot(@Nullable ItemEquipSlot equipSlot) {
            this.equipSlot = equipSlot;
            return this;
        }

        public Builder weaponType(@Nullable String weaponType) {
            this.weaponType = weaponType;
            return this;
        }

        public Builder armorType(@Nullable String armorType) {
            this.armorType = armorType;
            return this;
        }

        public Builder etcItemType(@Nullable String etcItemType) {
            this.etcItemType = etcItemType;
            return this;
        }

        public Builder stackable(@Nullable Boolean stackable) {
            this.stackable = stackable;
            return this;
        }

        public Builder questItem(@Nullable Boolean questItem) {
            this.questItem = questItem;
            return this;
        }

        public Builder petUsable(@Nullable Boolean petUsable) {
            this.petUsable = petUsable;
            return this;
        }

        public Builder defaultAction(@Nullable String defaultAction) {
            this.defaultAction = defaultAction;
            return this;
        }

        public Builder useHandler(@Nullable String useHandler) {
            this.useHandler = useHandler;
            return this;
        }

        public Builder duration(@Nullable Integer duration) {
            this.duration = duration;
            return this;
        }

        public Builder reuseDelay(@Nullable Integer reuseDelay) {
            this.reuseDelay = reuseDelay;
            return this;
        }

        public Builder skills(@Nullable List<ItemSkillRef> skills) {
            this.skills = skills;
            return this;
        }

        public Builder stats(@Nullable ItemStats stats) {
            this.stats = stats;
            return this;
        }

        public Builder restrictions(@Nullable ItemRestrictions restrictions) {
            this.restrictions = restrictions;
            return this;
        }

        public Builder upgrade(@Nullable ItemUpgrade upgrade) {
            this.upgrade = upgrade;
            return this;
        }

        public ItemTemplate build() {
            return new ItemTemplate(this);
        }
    }
}
