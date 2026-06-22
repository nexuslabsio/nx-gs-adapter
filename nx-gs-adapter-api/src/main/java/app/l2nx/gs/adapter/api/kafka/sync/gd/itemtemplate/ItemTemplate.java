package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import app.l2nx.gs.adapter.api.domain.WeaponType;
import app.l2nx.gs.adapter.api.domain.item.ArmorType;
import app.l2nx.gs.adapter.api.domain.item.EtcItemType;
import app.l2nx.gs.adapter.api.domain.item.ItemClass;
import app.l2nx.gs.adapter.api.domain.item.ItemEquipSlot;
import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.WellKnownGearScoreEnchantProfiles;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Build-agnostic item-template wire DTO — the common L2 denominator for static
 * item data, carried as the payload of {@code GameDataSyncEvent} on the {@code gd}
 * (game-data) sync stream's {@code itemtemplate} entity topic. Each host build
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
 * <p><b>Vocabulary:</b> {@link #getType()}, {@link #getEquipSlot()}, {@link #getWeaponType()},
 * {@link #getArmorType()} and {@link #getEtcItemType()} are closed domain enums
 * ({@link ItemClass}, {@link ItemEquipSlot}, {@link WeaponType}, {@link ArmorType},
 * {@link EtcItemType}) — the provider maps its core's internal enums/bitmasks onto them, or
 * yields {@code null} when a source value has no canonical counterpart. The remaining
 * open-string vocabulary fields ({@code material}, {@code grade}, {@code defaultAction},
 * {@code useHandler}) draw from a platform-canonical vocabulary in {@code UPPER_SNAKE_CASE}.</p>
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
    private final @Nullable Integer weight;
    private final @Nullable Long referencePrice;
    private final @Nullable String material;
    private final @Nullable String grade;
    private final @Nullable ItemEquipSlot equipSlot;
    private final @Nullable WeaponType weaponType;
    private final @Nullable ArmorType armorType;
    private final @Nullable EtcItemType etcItemType;
    private final @Nullable Boolean stackable;
    private final @Nullable Boolean questItem;
    private final @Nullable Boolean petUsable;
    private final @Nullable String defaultAction;
    private final @Nullable String useHandler;
    private final @Nullable Integer duration;
    private final @Nullable Integer reuseDelayMs;
    private final @Nullable List<ItemSkillRef> skills;
    private final @Nullable ItemStats stats;
    private final @Nullable ItemRestrictions restrictions;
    private final @Nullable ItemUpgrade upgrade;
    private final @Nullable Integer gearScore;
    private final @Nullable String gearScoreEnchantProfile;

    public ItemTemplate(
            int id,
            ItemClass type,
            @Nullable String icon,
            @Nullable LocalizedText name,
            @Nullable Integer weight,
            @Nullable Long referencePrice,
            @Nullable String material,
            @Nullable String grade,
            @Nullable ItemEquipSlot equipSlot,
            @Nullable WeaponType weaponType,
            @Nullable ArmorType armorType,
            @Nullable EtcItemType etcItemType,
            @Nullable Boolean stackable,
            @Nullable Boolean questItem,
            @Nullable Boolean petUsable,
            @Nullable String defaultAction,
            @Nullable String useHandler,
            @Nullable Integer duration,
            @Nullable Integer reuseDelayMs,
            @Nullable List<ItemSkillRef> skills,
            @Nullable ItemStats stats,
            @Nullable ItemRestrictions restrictions,
            @Nullable ItemUpgrade upgrade,
            @Nullable Integer gearScore,
            @Nullable String gearScoreEnchantProfile) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.icon = icon;
        this.name = name;
        this.weight = weight;
        this.referencePrice = referencePrice;
        this.material = material;
        this.grade = grade;
        this.equipSlot = equipSlot;
        this.weaponType = weaponType;
        this.armorType = armorType;
        this.etcItemType = etcItemType;
        this.stackable = stackable;
        this.questItem = questItem;
        this.petUsable = petUsable;
        this.defaultAction = defaultAction;
        this.useHandler = useHandler;
        this.duration = duration;
        this.reuseDelayMs = reuseDelayMs;
        this.skills = skills == null ? null : Collections.unmodifiableList(new ArrayList<ItemSkillRef>(skills));
        this.stats = stats;
        this.restrictions = restrictions;
        this.upgrade = upgrade;
        this.gearScore = gearScore;
        this.gearScoreEnchantProfile = gearScoreEnchantProfile;
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

    public @Nullable WeaponType getWeaponType() {
        return weaponType;
    }

    public @Nullable ArmorType getArmorType() {
        return armorType;
    }

    public @Nullable EtcItemType getEtcItemType() {
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

    public @Nullable Integer getReuseDelayMs() {
        return reuseDelayMs;
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

    /**
     * Base gear-score contribution of this item — the build-defined "power"
     * weight the item adds before enchant / attribute scaling. {@code null} when
     * the build does not compute gear score for the item (host sentinel
     * {@code -1} maps to {@code null}).
     */
    public @Nullable Integer getGearScore() {
        return gearScore;
    }

    /**
     * Optional open-string profile key governing how this item's gear score grows
     * with enchant level — a reference into the gear-score ruleset
     * ({@code gearscore} entity) rather than an inline table: it matches the rule
     * with the same {@code key} in the ruleset's {@code ENCHANT_PROFILE} group.
     * Canonical {@code UPPER_SNAKE_CASE} values are in
     * {@link WellKnownGearScoreEnchantProfiles}. {@code null} when the build has no
     * gear score or no profile concept.
     */
    public @Nullable String getGearScoreEnchantProfile() {
        return gearScoreEnchantProfile;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .type(type)
                .icon(icon)
                .name(name)
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
                .reuseDelayMs(reuseDelayMs)
                .skills(skills)
                .stats(stats)
                .restrictions(restrictions)
                .upgrade(upgrade)
                .gearScore(gearScore)
                .gearScoreEnchantProfile(gearScoreEnchantProfile);
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
                && Objects.equals(reuseDelayMs, that.reuseDelayMs)
                && Objects.equals(skills, that.skills)
                && Objects.equals(stats, that.stats)
                && Objects.equals(restrictions, that.restrictions)
                && Objects.equals(upgrade, that.upgrade)
                && Objects.equals(gearScore, that.gearScore)
                && Objects.equals(gearScoreEnchantProfile, that.gearScoreEnchantProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                type,
                icon,
                name,
                weight,
                referencePrice,
                material,
                grade,
                equipSlot,
                weaponType,
                armorType,
                etcItemType,
                stackable,
                questItem,
                petUsable,
                defaultAction,
                useHandler,
                duration,
                reuseDelayMs,
                skills,
                stats,
                restrictions,
                upgrade,
                gearScore,
                gearScoreEnchantProfile);
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
        private @Nullable Integer weight;
        private @Nullable Long referencePrice;
        private @Nullable String material;
        private @Nullable String grade;
        private @Nullable ItemEquipSlot equipSlot;
        private @Nullable WeaponType weaponType;
        private @Nullable ArmorType armorType;
        private @Nullable EtcItemType etcItemType;
        private @Nullable Boolean stackable;
        private @Nullable Boolean questItem;
        private @Nullable Boolean petUsable;
        private @Nullable String defaultAction;
        private @Nullable String useHandler;
        private @Nullable Integer duration;
        private @Nullable Integer reuseDelayMs;
        private @Nullable List<ItemSkillRef> skills;
        private @Nullable ItemStats stats;
        private @Nullable ItemRestrictions restrictions;
        private @Nullable ItemUpgrade upgrade;
        private @Nullable Integer gearScore;
        private @Nullable String gearScoreEnchantProfile;

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

        public Builder weaponType(@Nullable WeaponType weaponType) {
            this.weaponType = weaponType;
            return this;
        }

        public Builder armorType(@Nullable ArmorType armorType) {
            this.armorType = armorType;
            return this;
        }

        public Builder etcItemType(@Nullable EtcItemType etcItemType) {
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

        public Builder reuseDelayMs(@Nullable Integer reuseDelayMs) {
            this.reuseDelayMs = reuseDelayMs;
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

        public Builder gearScore(@Nullable Integer gearScore) {
            this.gearScore = gearScore;
            return this;
        }

        public Builder gearScoreEnchantProfile(@Nullable String gearScoreEnchantProfile) {
            this.gearScoreEnchantProfile = gearScoreEnchantProfile;
            return this;
        }

        public ItemTemplate build() {
            return new ItemTemplate(
                    id,
                    type,
                    icon,
                    name,
                    weight,
                    referencePrice,
                    material,
                    grade,
                    equipSlot,
                    weaponType,
                    armorType,
                    etcItemType,
                    stackable,
                    questItem,
                    petUsable,
                    defaultAction,
                    useHandler,
                    duration,
                    reuseDelayMs,
                    skills,
                    stats,
                    restrictions,
                    upgrade,
                    gearScore,
                    gearScoreEnchantProfile);
        }
    }
}
