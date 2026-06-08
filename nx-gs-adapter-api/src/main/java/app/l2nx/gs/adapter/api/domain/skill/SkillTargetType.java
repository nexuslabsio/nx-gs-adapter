package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Canonical, build-agnostic skill target-type vocabulary — the closed set a skill's
 * {@code TARGET} characteristic may carry. A shared skill-domain enum (not tied to one
 * wire DTO); a host provider maps its core's internal target-type enum onto these tokens
 * (e.g. an L2J/L2E {@code TargetType}: {@code AREA → AOE}, {@code HOLY → SIEGE_HOLY},
 * {@code PARTY_NOTME → AOE_PARTY_NOT_ME}). Canonical names are prefixed for clarity —
 * {@code AOE_*} = area around the target, {@code AURA_*} = area around the caster,
 * {@code SIEGE_*} = castle/fort siege objects — so they intentionally differ from raw
 * core names; the mapping lives in the provider, not here.
 *
 * <p>Groups (for reference):</p>
 * <ul>
 *   <li><b>Single:</b> {@link #ONE}, {@link #SELF}, {@link #NONE}, {@link #ENEMY_ONLY}.</li>
 *   <li><b>AoE around target:</b> {@link #AOE}, {@link #AOE_FRONT}, {@link #AOE_BEHIND},
 *       {@link #AOE_FRIENDLY}, {@link #AOE_SUMMON}, {@link #AOE_CORPSE_MOB}, {@link #AOE_MOB}.</li>
 *   <li><b>Aura around caster:</b> {@link #AURA}, {@link #AURA_FRONT}, {@link #AURA_BEHIND},
 *       {@link #AURA_FRIENDLY}, {@link #AURA_CORPSE_MOB}, {@link #AURA_UNDEAD_ENEMY}.</li>
 *   <li><b>Party / clan / alliance:</b> {@link #PARTY}, {@link #PARTY_MEMBER},
 *       {@link #PARTY_OTHER}, {@link #AOE_PARTY_NOT_ME}, {@link #PARTY_CLAN}, {@link #CLAN},
 *       {@link #CLAN_MEMBER}, {@link #ALLY}, {@link #COMMAND_CHANNEL}.</li>
 *   <li><b>Summons / pets:</b> {@link #SUMMON}, {@link #SERVITOR}, {@link #PET},
 *       {@link #PET_OWNER}, {@link #SUMMON_ENEMY}.</li>
 *   <li><b>Corpses:</b> {@link #CORPSE}, {@link #CORPSE_PLAYER}, {@link #CORPSE_MOB},
 *       {@link #CORPSE_PET}, {@link #CORPSE_PARTY}, {@link #CORPSE_CLAN}, {@link #CORPSE_ALLY}.</li>
 *   <li><b>Siege / special:</b> {@link #SIEGE_HOLY}, {@link #SIEGE_FLAGPOLE},
 *       {@link #UNLOCKABLE}, {@link #GROUND}.</li>
 * </ul>
 */
public enum SkillTargetType {

    // Single
    ONE,
    SELF,
    NONE,
    ENEMY_ONLY,

    // AoE around the target
    AOE,
    AOE_FRONT,
    AOE_BEHIND,
    AOE_FRIENDLY,
    AOE_SUMMON,
    AOE_CORPSE_MOB,
    AOE_MOB,

    // Aura around the caster
    AURA,
    AURA_FRONT,
    AURA_BEHIND,
    AURA_FRIENDLY,
    AURA_CORPSE_MOB,
    AURA_UNDEAD_ENEMY,

    // Party / clan / alliance
    PARTY,
    PARTY_MEMBER,
    PARTY_OTHER,
    AOE_PARTY_NOT_ME,
    PARTY_CLAN,
    CLAN,
    CLAN_MEMBER,
    ALLY,
    COMMAND_CHANNEL,

    // Summons / pets
    SUMMON,
    SERVITOR,
    PET,
    PET_OWNER,
    SUMMON_ENEMY,

    // Corpses
    CORPSE,
    CORPSE_PLAYER,
    CORPSE_MOB,
    CORPSE_PET,
    CORPSE_PARTY,
    CORPSE_CLAN,
    CORPSE_ALLY,

    // Siege / special
    SIEGE_HOLY,
    SIEGE_FLAGPOLE,
    UNLOCKABLE,
    GROUND
}
