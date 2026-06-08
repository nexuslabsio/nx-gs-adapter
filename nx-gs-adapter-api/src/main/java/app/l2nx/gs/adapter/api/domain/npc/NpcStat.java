package app.l2nx.gs.adapter.api.domain.npc;

/**
 * Canonical, build-agnostic npc-characteristic vocabulary — the closed set of keys that
 * may appear in an npc's typed-characteristics ({@code stats}) bag. A shared npc-domain
 * enum (not tied to one wire DTO); the wiki assembles the bag read-side from {@code gd_*}
 * columns, keying each entry by {@code name()}. All values are {@code NUMBER}; elemental
 * attack/defence are keyed separately by {@code Attribute}.
 *
 * <p>Contract is ready; npc characteristics are wired in a later phase (after item + skill).</p>
 */
public enum NpcStat {

    // Vitals & regen
    MAX_HP,
    MAX_MP,
    HP_REG,
    MP_REG,

    // Offense / defense
    P_ATK,
    P_DEF,
    M_ATK,
    M_DEF,
    P_ATK_SPD,
    M_ATK_SPD,
    ATK_RANGE,
    CRIT_RATE,
    SHIELD_DEF,
    SHIELD_RATE,

    // Movement
    RUN_SPEED,
    WALK_SPEED,

    // Base stats
    STR,
    DEX,
    CON,
    INT,
    WIT,
    MEN,

    // Rewards
    REWARD_EXP,
    REWARD_SP,
    REWARD_RP
}
