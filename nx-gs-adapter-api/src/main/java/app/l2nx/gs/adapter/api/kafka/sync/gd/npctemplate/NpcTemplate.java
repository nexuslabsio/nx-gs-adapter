package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.npc.NpcRace;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Build-agnostic NPC-template wire DTO — the common L2 denominator for static NPC
 * data, carried as the payload of {@code GameDataSyncEvent} on the {@code gd}
 * (game-data) sync stream's {@code npc} entity topic. Each host build supplies its
 * own provider mapping its core's internal NPC representation into this shape;
 * nothing here names a specific core.
 *
 * <p><b>Nullability:</b> only {@link #getId()} and {@link #getType()} are non-null.
 * Every other field is {@link Nullable} (former primitives boxed) so {@code null}
 * means "this build did not supply it". {@code type} is the host's server-side NPC
 * type (e.g. {@code Monster} / {@code RaidBoss}); kept as an open string since the
 * set is large and fork-dependent.</p>
 *
 * <p><b>Functional grouping:</b> cohesive areas are nested objects — {@link #getStats()}
 * (combat / progression numbers), {@link #getAttributes()} (STR/DEX/…),
 * {@link #getAttribute()} (elemental attack/defence). The list-collections
 * ({@link #getSkills()}, {@link #getDrops()}, {@link #getMinions()},
 * {@link #getAbsorbs()}, {@link #getSpawns()}) ride the same message and are
 * fanned out into child rows on the consumer side.</p>
 *
 * <p>Sourced from the host's already-parsed in-memory templates only — client-patch
 * visual fields (client npc type, mesh, icon, class name, draw scale, nick colour)
 * are a later slice and intentionally absent here. The race-marker skill (id
 * {@code 4416} on most cores) is consumed into {@link #getRace()} and not repeated
 * in {@link #getSkills()}.</p>
 */
public final class NpcTemplate {

    private final int id;
    private final String type;
    private final @Nullable Integer displayId;
    private final @Nullable Integer level;
    private final @Nullable NpcRace race;
    private final @Nullable String aiType;
    private final @Nullable String shots;
    private final @Nullable String texture;
    private final @Nullable String raceIcon;
    private final @Nullable Integer aggroRange;
    private final @Nullable Double collisionRadius;
    private final @Nullable Double collisionHeight;
    private final @Nullable Boolean randomMinions;
    private final @Nullable LocalizedText name;
    private final @Nullable LocalizedText title;
    private final @Nullable Integer rightHand;
    private final @Nullable Integer leftHand;
    private final @Nullable NpcStats stats;
    private final @Nullable NpcBaseAttributes attributes;
    private final @Nullable NpcAttribute attribute;
    private final @Nullable List<NpcSkillRef> skills;
    private final @Nullable List<NpcDropGroup> drops;
    private final @Nullable List<NpcMinionRef> minions;
    private final @Nullable List<NpcAbsorb> absorbs;
    private final @Nullable List<NpcSpawn> spawns;

    public NpcTemplate(int id,
                       String type,
                       @Nullable Integer displayId,
                       @Nullable Integer level,
                       @Nullable NpcRace race,
                       @Nullable String aiType,
                       @Nullable String shots,
                       @Nullable String texture,
                       @Nullable String raceIcon,
                       @Nullable Integer aggroRange,
                       @Nullable Double collisionRadius,
                       @Nullable Double collisionHeight,
                       @Nullable Boolean randomMinions,
                       @Nullable LocalizedText name,
                       @Nullable LocalizedText title,
                       @Nullable Integer rightHand,
                       @Nullable Integer leftHand,
                       @Nullable NpcStats stats,
                       @Nullable NpcBaseAttributes attributes,
                       @Nullable NpcAttribute attribute,
                       @Nullable List<NpcSkillRef> skills,
                       @Nullable List<NpcDropGroup> drops,
                       @Nullable List<NpcMinionRef> minions,
                       @Nullable List<NpcAbsorb> absorbs,
                       @Nullable List<NpcSpawn> spawns) {
        this.id = id;
        this.type = Objects.requireNonNull(type, "type");
        this.displayId = displayId;
        this.level = level;
        this.race = race;
        this.aiType = aiType;
        this.shots = shots;
        this.texture = texture;
        this.raceIcon = raceIcon;
        this.aggroRange = aggroRange;
        this.collisionRadius = collisionRadius;
        this.collisionHeight = collisionHeight;
        this.randomMinions = randomMinions;
        this.name = name;
        this.title = title;
        this.rightHand = rightHand;
        this.leftHand = leftHand;
        this.stats = stats;
        this.attributes = attributes;
        this.attribute = attribute;
        this.skills = skills == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcSkillRef>(skills));
        this.drops = drops == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcDropGroup>(drops));
        this.minions = minions == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcMinionRef>(minions));
        this.absorbs = absorbs == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcAbsorb>(absorbs));
        this.spawns = spawns == null ? null
                : Collections.unmodifiableList(new ArrayList<NpcSpawn>(spawns));
    }

    public int getId() {
        return id;
    }

    /**
     * Server-side NPC type (open string, e.g. {@code Monster} / {@code RaidBoss}).
     */
    public String getType() {
        return type;
    }

    /**
     * Visual-template id (NPC renders as another); {@code null} = renders as itself.
     */
    public @Nullable Integer getDisplayId() {
        return displayId;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    public @Nullable NpcRace getRace() {
        return race;
    }

    /**
     * AI behaviour type (open string, e.g. {@code Fighter}).
     */
    public @Nullable String getAiType() {
        return aiType;
    }

    /**
     * Shot grade the NPC uses (open string, e.g. {@code NONE} / {@code SOUL} / {@code SPIRIT}).
     */
    public @Nullable String getShots() {
        return shots;
    }

    /**
     * Server-side visual class / texture name.
     */
    public @Nullable String getTexture() {
        return texture;
    }

    /**
     * Icon of the NPC's race marker (resolved from the race-marker skill's per-level icon);
     * {@code null} if the NPC has no race marker.
     */
    public @Nullable String getRaceIcon() {
        return raceIcon;
    }

    public @Nullable Integer getAggroRange() {
        return aggroRange;
    }

    public @Nullable Double getCollisionRadius() {
        return collisionRadius;
    }

    public @Nullable Double getCollisionHeight() {
        return collisionHeight;
    }

    /**
     * Whether the NPC's minions spawn from a random pool (vs the fixed minion list).
     */
    public @Nullable Boolean getRandomMinions() {
        return randomMinions;
    }

    public @Nullable LocalizedText getName() {
        return name;
    }

    /**
     * Localized title / nick shown under the name; {@code null} if none.
     */
    public @Nullable LocalizedText getTitle() {
        return title;
    }

    /**
     * Item id equipped in the right hand; {@code null} if unarmed.
     */
    public @Nullable Integer getRightHand() {
        return rightHand;
    }

    /**
     * Item id equipped in the left hand; {@code null} if none.
     */
    public @Nullable Integer getLeftHand() {
        return leftHand;
    }

    /**
     * Combat / progression stats; {@code null} if the build supplied none.
     */
    public @Nullable NpcStats getStats() {
        return stats;
    }

    /**
     * Base attributes (STR/DEX/CON/INT/WIT/MEN); {@code null} if none.
     */
    public @Nullable NpcBaseAttributes getAttributes() {
        return attributes;
    }

    /**
     * Elemental attack / defence; {@code null} if none.
     */
    public @Nullable NpcAttribute getAttribute() {
        return attribute;
    }

    /**
     * Skills the NPC has ({@code id}+{@code level} refs); {@code null} if none.
     * Excludes the race-marker skill (consumed into {@link #getRace()}).
     */
    public @Nullable List<NpcSkillRef> getSkills() {
        return skills;
    }

    /**
     * Drop / spoil reward groups; {@code null} if none.
     */
    public @Nullable List<NpcDropGroup> getDrops() {
        return drops;
    }

    /**
     * Leader→minion relationships; {@code null} if none.
     */
    public @Nullable List<NpcMinionRef> getMinions() {
        return minions;
    }

    /**
     * Soul-absorb rules; {@code null} if none.
     */
    public @Nullable List<NpcAbsorb> getAbsorbs() {
        return absorbs;
    }

    /**
     * Spawn definitions placing this NPC in the world; {@code null} if none.
     */
    public @Nullable List<NpcSpawn> getSpawns() {
        return spawns;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .type(type)
                .displayId(displayId)
                .level(level)
                .race(race)
                .aiType(aiType)
                .shots(shots)
                .texture(texture)
                .raceIcon(raceIcon)
                .aggroRange(aggroRange)
                .collisionRadius(collisionRadius)
                .collisionHeight(collisionHeight)
                .randomMinions(randomMinions)
                .name(name)
                .title(title)
                .rightHand(rightHand)
                .leftHand(leftHand)
                .stats(stats)
                .attributes(attributes)
                .attribute(attribute)
                .skills(skills)
                .drops(drops)
                .minions(minions)
                .absorbs(absorbs)
                .spawns(spawns);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcTemplate)) return false;
        NpcTemplate that = (NpcTemplate) o;
        return id == that.id
                && Objects.equals(type, that.type)
                && Objects.equals(displayId, that.displayId)
                && Objects.equals(level, that.level)
                && race == that.race
                && Objects.equals(aiType, that.aiType)
                && Objects.equals(shots, that.shots)
                && Objects.equals(texture, that.texture)
                && Objects.equals(raceIcon, that.raceIcon)
                && Objects.equals(aggroRange, that.aggroRange)
                && Objects.equals(collisionRadius, that.collisionRadius)
                && Objects.equals(collisionHeight, that.collisionHeight)
                && Objects.equals(randomMinions, that.randomMinions)
                && Objects.equals(name, that.name)
                && Objects.equals(title, that.title)
                && Objects.equals(rightHand, that.rightHand)
                && Objects.equals(leftHand, that.leftHand)
                && Objects.equals(stats, that.stats)
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(attribute, that.attribute)
                && Objects.equals(skills, that.skills)
                && Objects.equals(drops, that.drops)
                && Objects.equals(minions, that.minions)
                && Objects.equals(absorbs, that.absorbs)
                && Objects.equals(spawns, that.spawns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, displayId, level, race, aiType, shots, texture, raceIcon, aggroRange,
                collisionRadius, collisionHeight, randomMinions, name, title, rightHand, leftHand,
                stats, attributes, attribute, skills, drops, minions, absorbs, spawns);
    }

    @Override
    public String toString() {
        return "NpcTemplate[id=" + id + ", type=" + type + ", level=" + level + ", race=" + race + "]";
    }

    public static final class Builder {
        private int id;
        private String type;
        private @Nullable Integer displayId;
        private @Nullable Integer level;
        private @Nullable NpcRace race;
        private @Nullable String aiType;
        private @Nullable String shots;
        private @Nullable String texture;
        private @Nullable String raceIcon;
        private @Nullable Integer aggroRange;
        private @Nullable Double collisionRadius;
        private @Nullable Double collisionHeight;
        private @Nullable Boolean randomMinions;
        private @Nullable LocalizedText name;
        private @Nullable LocalizedText title;
        private @Nullable Integer rightHand;
        private @Nullable Integer leftHand;
        private @Nullable NpcStats stats;
        private @Nullable NpcBaseAttributes attributes;
        private @Nullable NpcAttribute attribute;
        private @Nullable List<NpcSkillRef> skills;
        private @Nullable List<NpcDropGroup> drops;
        private @Nullable List<NpcMinionRef> minions;
        private @Nullable List<NpcAbsorb> absorbs;
        private @Nullable List<NpcSpawn> spawns;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder displayId(@Nullable Integer displayId) {
            this.displayId = displayId;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder race(@Nullable NpcRace race) {
            this.race = race;
            return this;
        }

        public Builder aiType(@Nullable String aiType) {
            this.aiType = aiType;
            return this;
        }

        public Builder shots(@Nullable String shots) {
            this.shots = shots;
            return this;
        }

        public Builder texture(@Nullable String texture) {
            this.texture = texture;
            return this;
        }

        public Builder raceIcon(@Nullable String raceIcon) {
            this.raceIcon = raceIcon;
            return this;
        }

        public Builder aggroRange(@Nullable Integer aggroRange) {
            this.aggroRange = aggroRange;
            return this;
        }

        public Builder collisionRadius(@Nullable Double collisionRadius) {
            this.collisionRadius = collisionRadius;
            return this;
        }

        public Builder collisionHeight(@Nullable Double collisionHeight) {
            this.collisionHeight = collisionHeight;
            return this;
        }

        public Builder randomMinions(@Nullable Boolean randomMinions) {
            this.randomMinions = randomMinions;
            return this;
        }

        public Builder name(@Nullable LocalizedText name) {
            this.name = name;
            return this;
        }

        public Builder title(@Nullable LocalizedText title) {
            this.title = title;
            return this;
        }

        public Builder rightHand(@Nullable Integer rightHand) {
            this.rightHand = rightHand;
            return this;
        }

        public Builder leftHand(@Nullable Integer leftHand) {
            this.leftHand = leftHand;
            return this;
        }

        public Builder stats(@Nullable NpcStats stats) {
            this.stats = stats;
            return this;
        }

        public Builder attributes(@Nullable NpcBaseAttributes attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder attribute(@Nullable NpcAttribute attribute) {
            this.attribute = attribute;
            return this;
        }

        public Builder skills(@Nullable List<NpcSkillRef> skills) {
            this.skills = skills;
            return this;
        }

        public Builder drops(@Nullable List<NpcDropGroup> drops) {
            this.drops = drops;
            return this;
        }

        public Builder minions(@Nullable List<NpcMinionRef> minions) {
            this.minions = minions;
            return this;
        }

        public Builder absorbs(@Nullable List<NpcAbsorb> absorbs) {
            this.absorbs = absorbs;
            return this;
        }

        public Builder spawns(@Nullable List<NpcSpawn> spawns) {
            this.spawns = spawns;
            return this;
        }

        public NpcTemplate build() {
            return new NpcTemplate(id, type, displayId, level, race, aiType, shots, texture, raceIcon,
                    aggroRange, collisionRadius, collisionHeight, randomMinions, name, title, rightHand,
                    leftHand, stats, attributes, attribute, skills, drops, minions, absorbs, spawns);
        }
    }
}
