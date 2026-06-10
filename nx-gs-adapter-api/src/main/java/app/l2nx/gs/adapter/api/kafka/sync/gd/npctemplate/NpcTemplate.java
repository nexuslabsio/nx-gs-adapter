package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.npc.NpcRace;
import app.l2nx.gs.adapter.api.domain.npc.NpcStat;
import app.l2nx.gs.adapter.api.domain.WeaponType;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * type (e.g. {@code MONSTER} / {@code RAID_BOSS}); kept as an open string since the
 * set is large and fork-dependent. Behaviour flags ({@link #getLethalImmune()},
 * {@link #getNoRandomWalk()}, …) are emitted only when {@code true} — {@code null}
 * reads as {@code false}/unknown.</p>
 *
 * <p><b>Stats:</b> every numeric stat rides {@link #getStats()} — a single map keyed
 * by the canonical {@link NpcStat} token names (vitals, combat numbers, movement
 * speeds, base attributes, elemental power/resist, aggro range). The attack type is
 * not a magnitude and rides {@link #getAtkType()} as a {@link WeaponType} token.
 * Rewards are not stats and stay top-level ({@link #getRewardExp()} /
 * {@link #getRewardSp()} / {@link #getRewardRp()}).</p>
 *
 * <p>The list-collections ({@link #getSkills()}, {@link #getDrops()},
 * {@link #getMinions()}, {@link #getAbsorbs()}, {@link #getSpawns()}) ride the same
 * message and are fanned out into child rows on the consumer side.</p>
 *
 * <p>Sourced from the host's already-parsed in-memory templates only — client-patch
 * visual fields (client npc type, mesh, icon, class name, draw scale, nick colour)
 * are owned by the patch ingester and intentionally absent here. The race-marker
 * skill (id {@code 4416} on most cores) is consumed into {@link #getRace()} and not
 * repeated in {@link #getSkills()}.</p>
 */
public final class NpcTemplate {

    private final int id;
    private final String type;
    private final @Nullable Integer displayId;
    private final @Nullable Integer level;
    private final @Nullable NpcRace race;
    private final @Nullable String aiType;
    private final @Nullable String shots;
    private final @Nullable Boolean randomMinions;
    private final @Nullable Boolean lethalImmune;
    private final @Nullable Boolean championEligible;
    private final @Nullable Boolean noRandomWalk;
    private final @Nullable Boolean movementDisabled;
    private final @Nullable Integer maxPursueRange;
    private final @Nullable Boolean canSeeInSilentMove;
    private final @Nullable Boolean globalAggro;
    private final @Nullable String raceIcon;
    private final @Nullable Double collisionRadius;
    private final @Nullable Double collisionHeight;
    private final @Nullable String atkType;
    private final @Nullable Map<String, Double> stats;
    private final @Nullable Long rewardExp;
    private final @Nullable Long rewardSp;
    private final @Nullable Integer rewardRp;
    private final @Nullable NpcFaction faction;
    private final @Nullable Integer transformOnDeadNpcTemplateId;
    private final @Nullable Integer transformChancePercent;
    private final @Nullable Integer spawnOnDeathCount;
    private final @Nullable Integer spawnOnDeathChancePercent;
    private final @Nullable LocalizedText name;
    private final @Nullable LocalizedText title;
    private final @Nullable Integer rightHand;
    private final @Nullable Integer leftHand;
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
                       @Nullable Boolean randomMinions,
                       @Nullable Boolean lethalImmune,
                       @Nullable Boolean championEligible,
                       @Nullable Boolean noRandomWalk,
                       @Nullable Boolean movementDisabled,
                       @Nullable Integer maxPursueRange,
                       @Nullable Boolean canSeeInSilentMove,
                       @Nullable Boolean globalAggro,
                       @Nullable String raceIcon,
                       @Nullable Double collisionRadius,
                       @Nullable Double collisionHeight,
                       @Nullable String atkType,
                       @Nullable Map<String, Double> stats,
                       @Nullable Long rewardExp,
                       @Nullable Long rewardSp,
                       @Nullable Integer rewardRp,
                       @Nullable NpcFaction faction,
                       @Nullable Integer transformOnDeadNpcTemplateId,
                       @Nullable Integer transformChancePercent,
                       @Nullable Integer spawnOnDeathCount,
                       @Nullable Integer spawnOnDeathChancePercent,
                       @Nullable LocalizedText name,
                       @Nullable LocalizedText title,
                       @Nullable Integer rightHand,
                       @Nullable Integer leftHand,
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
        this.randomMinions = randomMinions;
        this.lethalImmune = lethalImmune;
        this.championEligible = championEligible;
        this.noRandomWalk = noRandomWalk;
        this.movementDisabled = movementDisabled;
        this.maxPursueRange = maxPursueRange;
        this.canSeeInSilentMove = canSeeInSilentMove;
        this.globalAggro = globalAggro;
        this.raceIcon = raceIcon;
        this.collisionRadius = collisionRadius;
        this.collisionHeight = collisionHeight;
        this.atkType = atkType;
        this.stats = stats == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, Double>(stats));
        this.rewardExp = rewardExp;
        this.rewardSp = rewardSp;
        this.rewardRp = rewardRp;
        this.faction = faction;
        this.transformOnDeadNpcTemplateId = transformOnDeadNpcTemplateId;
        this.transformChancePercent = transformChancePercent;
        this.spawnOnDeathCount = spawnOnDeathCount;
        this.spawnOnDeathChancePercent = spawnOnDeathChancePercent;
        this.name = name;
        this.title = title;
        this.rightHand = rightHand;
        this.leftHand = leftHand;
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
     * Server-side NPC type (open string, e.g. {@code MONSTER} / {@code RAID_BOSS}).
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
     * AI behaviour type (open string, e.g. {@code FIGHTER}).
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
     * Whether the NPC's minions spawn from a random pool (vs the fixed minion list).
     */
    public @Nullable Boolean getRandomMinions() {
        return randomMinions;
    }

    /**
     * Immune to lethal-strike effects; emitted only when {@code true}.
     */
    public @Nullable Boolean getLethalImmune() {
        return lethalImmune;
    }

    /**
     * Whether the npc can roll as a champion mob. Carried only when {@code false} — the
     * datapack's explicit {@code noChampion} exclusions; {@code null} reads as eligible.
     */
    public @Nullable Boolean getChampionEligible() {
        return championEligible;
    }

    /**
     * Does not wander away from its spawn point; emitted only when {@code true}.
     */
    public @Nullable Boolean getNoRandomWalk() {
        return noRandomWalk;
    }

    /**
     * Cannot move at all; emitted only when {@code true}.
     */
    public @Nullable Boolean getMovementDisabled() {
        return movementDisabled;
    }

    /**
     * Maximum pursuit distance from the spawn point, world units. Carried only when the
     * template sets it explicitly — server-config defaults are not materialized.
     */
    public @Nullable Integer getMaxPursueRange() {
        return maxPursueRange;
    }

    /**
     * Detects players sneaking with Silent Move; emitted only when {@code true}.
     */
    public @Nullable Boolean getCanSeeInSilentMove() {
        return canSeeInSilentMove;
    }

    /**
     * Aggroes regardless of distance (global aggro); emitted only when {@code true}.
     */
    public @Nullable Boolean getGlobalAggro() {
        return globalAggro;
    }

    /**
     * Icon of the NPC's race marker (resolved from the race-marker skill's per-level icon);
     * {@code null} if the NPC has no race marker.
     */
    public @Nullable String getRaceIcon() {
        return raceIcon;
    }

    public @Nullable Double getCollisionRadius() {
        return collisionRadius;
    }

    public @Nullable Double getCollisionHeight() {
        return collisionHeight;
    }

    /**
     * Attack weapon kind as a canonical {@link WeaponType} token
     * ({@code SWORD} / {@code BOW} / {@code DUAL_FIST} / …).
     */
    public @Nullable String getAtkType() {
        return atkType;
    }

    /**
     * Every numeric stat the NPC carries, keyed by the canonical {@link NpcStat} token name
     * (e.g. {@code MAX_HP}, {@code P_ATK}, {@code AGGRO_RANGE}, {@code FIRE_RES}). Zero
     * values are dropped by the producer ("not applicable"); {@code null} when the build
     * supplied no stats.
     */
    public @Nullable Map<String, Double> getStats() {
        return stats;
    }

    /**
     * Experience reward on kill — raw template value, no server rates applied.
     */
    public @Nullable Long getRewardExp() {
        return rewardExp;
    }

    public @Nullable Long getRewardSp() {
        return rewardSp;
    }

    public @Nullable Integer getRewardRp() {
        return rewardRp;
    }

    /**
     * Social clan — same-faction NPCs within the faction range assist each other;
     * {@code null} when the NPC belongs to no faction.
     */
    public @Nullable NpcFaction getFaction() {
        return faction;
    }

    /**
     * NPC template this one transforms into on death; {@code null} when it does not
     * transform.
     */
    public @Nullable Integer getTransformOnDeadNpcTemplateId() {
        return transformOnDeadNpcTemplateId;
    }

    /**
     * Chance of the on-death transform, percent; carried only alongside
     * {@link #getTransformOnDeadNpcTemplateId()}.
     */
    public @Nullable Integer getTransformChancePercent() {
        return transformChancePercent;
    }

    /**
     * Number of extra NPCs spawned on death. The spawned template id lives in the host's
     * AI script, not in the template, and is not carried.
     */
    public @Nullable Integer getSpawnOnDeathCount() {
        return spawnOnDeathCount;
    }

    /**
     * Chance of the on-death extra spawn, percent.
     */
    public @Nullable Integer getSpawnOnDeathChancePercent() {
        return spawnOnDeathChancePercent;
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
                .randomMinions(randomMinions)
                .lethalImmune(lethalImmune)
                .championEligible(championEligible)
                .noRandomWalk(noRandomWalk)
                .movementDisabled(movementDisabled)
                .maxPursueRange(maxPursueRange)
                .canSeeInSilentMove(canSeeInSilentMove)
                .globalAggro(globalAggro)
                .raceIcon(raceIcon)
                .collisionRadius(collisionRadius)
                .collisionHeight(collisionHeight)
                .atkType(atkType)
                .stats(stats)
                .rewardExp(rewardExp)
                .rewardSp(rewardSp)
                .rewardRp(rewardRp)
                .faction(faction)
                .transformOnDeadNpcTemplateId(transformOnDeadNpcTemplateId)
                .transformChancePercent(transformChancePercent)
                .spawnOnDeathCount(spawnOnDeathCount)
                .spawnOnDeathChancePercent(spawnOnDeathChancePercent)
                .name(name)
                .title(title)
                .rightHand(rightHand)
                .leftHand(leftHand)
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
                && Objects.equals(randomMinions, that.randomMinions)
                && Objects.equals(lethalImmune, that.lethalImmune)
                && Objects.equals(championEligible, that.championEligible)
                && Objects.equals(noRandomWalk, that.noRandomWalk)
                && Objects.equals(movementDisabled, that.movementDisabled)
                && Objects.equals(maxPursueRange, that.maxPursueRange)
                && Objects.equals(canSeeInSilentMove, that.canSeeInSilentMove)
                && Objects.equals(globalAggro, that.globalAggro)
                && Objects.equals(raceIcon, that.raceIcon)
                && Objects.equals(collisionRadius, that.collisionRadius)
                && Objects.equals(collisionHeight, that.collisionHeight)
                && Objects.equals(atkType, that.atkType)
                && Objects.equals(stats, that.stats)
                && Objects.equals(rewardExp, that.rewardExp)
                && Objects.equals(rewardSp, that.rewardSp)
                && Objects.equals(rewardRp, that.rewardRp)
                && Objects.equals(faction, that.faction)
                && Objects.equals(transformOnDeadNpcTemplateId, that.transformOnDeadNpcTemplateId)
                && Objects.equals(transformChancePercent, that.transformChancePercent)
                && Objects.equals(spawnOnDeathCount, that.spawnOnDeathCount)
                && Objects.equals(spawnOnDeathChancePercent, that.spawnOnDeathChancePercent)
                && Objects.equals(name, that.name)
                && Objects.equals(title, that.title)
                && Objects.equals(rightHand, that.rightHand)
                && Objects.equals(leftHand, that.leftHand)
                && Objects.equals(skills, that.skills)
                && Objects.equals(drops, that.drops)
                && Objects.equals(minions, that.minions)
                && Objects.equals(absorbs, that.absorbs)
                && Objects.equals(spawns, that.spawns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, displayId, level, race, aiType, shots, randomMinions,
                lethalImmune, championEligible, noRandomWalk, movementDisabled, maxPursueRange,
                canSeeInSilentMove, globalAggro, raceIcon, collisionRadius, collisionHeight,
                atkType, stats, rewardExp, rewardSp, rewardRp, faction,
                transformOnDeadNpcTemplateId, transformChancePercent, spawnOnDeathCount,
                spawnOnDeathChancePercent, name, title, rightHand, leftHand,
                skills, drops, minions, absorbs, spawns);
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
        private @Nullable Boolean randomMinions;
        private @Nullable Boolean lethalImmune;
        private @Nullable Boolean championEligible;
        private @Nullable Boolean noRandomWalk;
        private @Nullable Boolean movementDisabled;
        private @Nullable Integer maxPursueRange;
        private @Nullable Boolean canSeeInSilentMove;
        private @Nullable Boolean globalAggro;
        private @Nullable String raceIcon;
        private @Nullable Double collisionRadius;
        private @Nullable Double collisionHeight;
        private @Nullable String atkType;
        private @Nullable Map<String, Double> stats;
        private @Nullable Long rewardExp;
        private @Nullable Long rewardSp;
        private @Nullable Integer rewardRp;
        private @Nullable NpcFaction faction;
        private @Nullable Integer transformOnDeadNpcTemplateId;
        private @Nullable Integer transformChancePercent;
        private @Nullable Integer spawnOnDeathCount;
        private @Nullable Integer spawnOnDeathChancePercent;
        private @Nullable LocalizedText name;
        private @Nullable LocalizedText title;
        private @Nullable Integer rightHand;
        private @Nullable Integer leftHand;
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

        public Builder randomMinions(@Nullable Boolean randomMinions) {
            this.randomMinions = randomMinions;
            return this;
        }

        public Builder lethalImmune(@Nullable Boolean lethalImmune) {
            this.lethalImmune = lethalImmune;
            return this;
        }

        public Builder championEligible(@Nullable Boolean championEligible) {
            this.championEligible = championEligible;
            return this;
        }

        public Builder noRandomWalk(@Nullable Boolean noRandomWalk) {
            this.noRandomWalk = noRandomWalk;
            return this;
        }

        public Builder movementDisabled(@Nullable Boolean movementDisabled) {
            this.movementDisabled = movementDisabled;
            return this;
        }

        public Builder maxPursueRange(@Nullable Integer maxPursueRange) {
            this.maxPursueRange = maxPursueRange;
            return this;
        }

        public Builder canSeeInSilentMove(@Nullable Boolean canSeeInSilentMove) {
            this.canSeeInSilentMove = canSeeInSilentMove;
            return this;
        }

        public Builder globalAggro(@Nullable Boolean globalAggro) {
            this.globalAggro = globalAggro;
            return this;
        }

        public Builder raceIcon(@Nullable String raceIcon) {
            this.raceIcon = raceIcon;
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

        public Builder atkType(@Nullable String atkType) {
            this.atkType = atkType;
            return this;
        }

        public Builder stats(@Nullable Map<String, Double> stats) {
            this.stats = stats;
            return this;
        }

        public Builder rewardExp(@Nullable Long rewardExp) {
            this.rewardExp = rewardExp;
            return this;
        }

        public Builder rewardSp(@Nullable Long rewardSp) {
            this.rewardSp = rewardSp;
            return this;
        }

        public Builder rewardRp(@Nullable Integer rewardRp) {
            this.rewardRp = rewardRp;
            return this;
        }

        public Builder faction(@Nullable NpcFaction faction) {
            this.faction = faction;
            return this;
        }

        public Builder transformOnDeadNpcTemplateId(@Nullable Integer transformOnDeadNpcTemplateId) {
            this.transformOnDeadNpcTemplateId = transformOnDeadNpcTemplateId;
            return this;
        }

        public Builder transformChancePercent(@Nullable Integer transformChancePercent) {
            this.transformChancePercent = transformChancePercent;
            return this;
        }

        public Builder spawnOnDeathCount(@Nullable Integer spawnOnDeathCount) {
            this.spawnOnDeathCount = spawnOnDeathCount;
            return this;
        }

        public Builder spawnOnDeathChancePercent(@Nullable Integer spawnOnDeathChancePercent) {
            this.spawnOnDeathChancePercent = spawnOnDeathChancePercent;
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
            return new NpcTemplate(id, type, displayId, level, race, aiType, shots, randomMinions,
                    lethalImmune, championEligible, noRandomWalk, movementDisabled, maxPursueRange,
                    canSeeInSilentMove, globalAggro, raceIcon, collisionRadius, collisionHeight,
                    atkType, stats, rewardExp, rewardSp, rewardRp, faction,
                    transformOnDeadNpcTemplateId, transformChancePercent, spawnOnDeathCount,
                    spawnOnDeathChancePercent, name, title, rightHand, leftHand,
                    skills, drops, minions, absorbs, spawns);
        }
    }
}
