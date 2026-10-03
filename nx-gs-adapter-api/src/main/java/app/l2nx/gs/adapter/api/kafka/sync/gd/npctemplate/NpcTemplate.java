package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.WeaponType;
import app.l2nx.gs.adapter.api.domain.npc.NpcRace;
import app.l2nx.gs.adapter.api.domain.stat.Stat;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for static NPC data, payload of {@code GameDataSyncEvent} on the {@code npc} topic. Only
 * {@link #getId()} and {@link #getType()} are non-null; elsewhere {@code null} means the build did not
 * supply it, and behaviour flags are emitted only when {@code true} ({@code null} reads as false).
 *
 * <p>Numeric stats ride {@link #getStats()} keyed by {@link Stat} token; the attack type is a
 * {@link WeaponType} token. Client-patch visual fields are owned by the patch ingester and absent here.</p>
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

    public NpcTemplate(
            int id,
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
        this.stats = stats == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, Double>(stats));
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
        this.skills = skills == null ? null : Collections.unmodifiableList(new ArrayList<NpcSkillRef>(skills));
        this.drops = drops == null ? null : Collections.unmodifiableList(new ArrayList<NpcDropGroup>(drops));
        this.minions = minions == null ? null : Collections.unmodifiableList(new ArrayList<NpcMinionRef>(minions));
        this.absorbs = absorbs == null ? null : Collections.unmodifiableList(new ArrayList<NpcAbsorb>(absorbs));
        this.spawns = spawns == null ? null : Collections.unmodifiableList(new ArrayList<NpcSpawn>(spawns));
    }

    public int getId() {
        return id;
    }

    /**
     * Platform business taxonomy of the NPC, not the fork's raw engine class. Open string; for bosses the
     * host classifies {@link WellKnownNpcTypes#RAID_BOSS} vs {@link WellKnownNpcTypes#EPIC_BOSS}.
     */
    public String getType() {
        return type;
    }

    /**
     * Template whose visuals this NPC renders as; {@code null} = itself.
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

    public @Nullable String getAiType() {
        return aiType;
    }

    public @Nullable String getShots() {
        return shots;
    }

    /**
     * Minions spawn from a random pool (vs the fixed minion list).
     */
    public @Nullable Boolean getRandomMinions() {
        return randomMinions;
    }

    public @Nullable Boolean getLethalImmune() {
        return lethalImmune;
    }

    /**
     * Emitted only when {@code false} (datapack {@code noChampion}); {@code null} reads as eligible.
     */
    public @Nullable Boolean getChampionEligible() {
        return championEligible;
    }

    public @Nullable Boolean getNoRandomWalk() {
        return noRandomWalk;
    }

    public @Nullable Boolean getMovementDisabled() {
        return movementDisabled;
    }

    /**
     * World units; {@code null} when the template sets none (server-config default not materialized).
     */
    public @Nullable Integer getMaxPursueRange() {
        return maxPursueRange;
    }

    public @Nullable Boolean getCanSeeInSilentMove() {
        return canSeeInSilentMove;
    }

    public @Nullable Boolean getGlobalAggro() {
        return globalAggro;
    }

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
     * Canonical {@link WeaponType} token.
     */
    public @Nullable String getAtkType() {
        return atkType;
    }

    /**
     * Keyed by canonical {@link Stat} token name; zero values are dropped by the producer.
     */
    public @Nullable Map<String, Double> getStats() {
        return stats;
    }

    /**
     * Raw template value, no server rates applied.
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

    public @Nullable NpcFaction getFaction() {
        return faction;
    }

    public @Nullable Integer getTransformOnDeadNpcTemplateId() {
        return transformOnDeadNpcTemplateId;
    }

    public @Nullable Integer getTransformChancePercent() {
        return transformChancePercent;
    }

    /**
     * The spawned template id lives in the host's AI script and is not carried.
     */
    public @Nullable Integer getSpawnOnDeathCount() {
        return spawnOnDeathCount;
    }

    public @Nullable Integer getSpawnOnDeathChancePercent() {
        return spawnOnDeathChancePercent;
    }

    public @Nullable LocalizedText getName() {
        return name;
    }

    public @Nullable LocalizedText getTitle() {
        return title;
    }

    public @Nullable Integer getRightHand() {
        return rightHand;
    }

    public @Nullable Integer getLeftHand() {
        return leftHand;
    }

    /**
     * Excludes the race-marker skill, which is consumed into {@link #getRace()}.
     */
    public @Nullable List<NpcSkillRef> getSkills() {
        return skills;
    }

    public @Nullable List<NpcDropGroup> getDrops() {
        return drops;
    }

    public @Nullable List<NpcMinionRef> getMinions() {
        return minions;
    }

    public @Nullable List<NpcAbsorb> getAbsorbs() {
        return absorbs;
    }

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
        return Objects.hash(
                id,
                type,
                displayId,
                level,
                race,
                aiType,
                shots,
                randomMinions,
                lethalImmune,
                championEligible,
                noRandomWalk,
                movementDisabled,
                maxPursueRange,
                canSeeInSilentMove,
                globalAggro,
                raceIcon,
                collisionRadius,
                collisionHeight,
                atkType,
                stats,
                rewardExp,
                rewardSp,
                rewardRp,
                faction,
                transformOnDeadNpcTemplateId,
                transformChancePercent,
                spawnOnDeathCount,
                spawnOnDeathChancePercent,
                name,
                title,
                rightHand,
                leftHand,
                skills,
                drops,
                minions,
                absorbs,
                spawns);
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
            return new NpcTemplate(
                    id,
                    type,
                    displayId,
                    level,
                    race,
                    aiType,
                    shots,
                    randomMinions,
                    lethalImmune,
                    championEligible,
                    noRandomWalk,
                    movementDisabled,
                    maxPursueRange,
                    canSeeInSilentMove,
                    globalAggro,
                    raceIcon,
                    collisionRadius,
                    collisionHeight,
                    atkType,
                    stats,
                    rewardExp,
                    rewardSp,
                    rewardRp,
                    faction,
                    transformOnDeadNpcTemplateId,
                    transformChancePercent,
                    spawnOnDeathCount,
                    spawnOnDeathChancePercent,
                    name,
                    title,
                    rightHand,
                    leftHand,
                    skills,
                    drops,
                    minions,
                    absorbs,
                    spawns);
        }
    }
}
