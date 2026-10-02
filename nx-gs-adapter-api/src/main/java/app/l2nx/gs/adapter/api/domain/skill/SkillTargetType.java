package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Closed set of values a skill's {@code TARGET} characteristic may carry. Names differ from raw core
 * names ({@code AREA} to {@link #AOE}); the mapping lives in the provider. {@code AOE_*} = area around
 * the target, {@code AURA_*} = around the caster, {@code SIEGE_*} = siege objects.
 */
public enum SkillTargetType {
    ONE,
    SELF,
    NONE,
    ENEMY_ONLY,

    AOE,
    AOE_FRONT,
    AOE_BEHIND,
    AOE_FRIENDLY,
    AOE_SUMMON,
    AOE_CORPSE_MOB,
    AOE_MOB,

    AURA,
    AURA_FRONT,
    AURA_BEHIND,
    AURA_FRIENDLY,
    AURA_CORPSE_MOB,
    AURA_UNDEAD_ENEMY,

    PARTY,
    PARTY_MEMBER,
    PARTY_OTHER,
    AOE_PARTY_NOT_ME,
    PARTY_CLAN,
    CLAN,
    CLAN_MEMBER,
    ALLY,
    COMMAND_CHANNEL,

    SUMMON,
    SERVITOR,
    PET,
    PET_OWNER,
    SUMMON_ENEMY,

    CORPSE,
    CORPSE_PLAYER,
    CORPSE_MOB,
    CORPSE_PET,
    CORPSE_PARTY,
    CORPSE_CLAN,
    CORPSE_ALLY,

    SIEGE_HOLY,
    SIEGE_FLAGPOLE,
    UNLOCKABLE,
    GROUND
}
