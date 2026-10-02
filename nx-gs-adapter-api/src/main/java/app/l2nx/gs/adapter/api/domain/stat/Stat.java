package app.l2nx.gs.adapter.api.domain.stat;

/**
 * Closed set of keys of any {@code Map<String, Double>} stats bag on the wire (items, NPCs, skills).
 * Not every key applies to every entity; applicability is per-DTO.
 *
 * <p>{@code SPEED} (item bonus) coexists with {@code RUN_SPEED} / {@code WALK_SPEED} (NPC absolute), and
 * {@code AGGRO_POINTS} (skill threat) with {@code AGGRO_RANGE} (NPC radius): different dimensions, wire keys fixed.</p>
 */
public enum Stat {
    P_ATK,
    M_ATK,
    ATK_SPD,
    CAST_SPD,
    CRIT_RATE,
    M_CRIT_RATE,
    ACCURACY,
    ATK_RANGE,
    ATK_ANGLE,

    P_DEF,
    M_DEF,
    EVASION,
    SHIELD_DEF,
    SHIELD_RATE,
    M_SUCCESS_RES,

    MAX_HP,
    MAX_MP,
    HP_REGEN,
    MP_REGEN,

    SPEED,
    RUN_SPEED,
    WALK_SPEED,

    // Weapon mechanics (items only)
    RANDOM_DAMAGE,
    SOULSHOT_COUNT,
    SPIRITSHOT_COUNT,
    MAGIC_WEAPON,

    // Special / non-combat (items only)
    AUTOLOOT,
    INV_LIMIT,

    STR,
    DEX,
    CON,
    INT,
    WIT,
    MEN,

    FIRE_POWER,
    WATER_POWER,
    WIND_POWER,
    EARTH_POWER,
    HOLY_POWER,
    DARK_POWER,

    FIRE_RES,
    WATER_RES,
    WIND_RES,
    EARTH_RES,
    HOLY_RES,
    DARK_RES,

    AGGRO_RANGE,

    POWER,
    PVP_POWER,
    PVE_POWER,
    MAGIC_LEVEL,
    BLOW_CHANCE,
    LETHAL_STRIKE_RATE,
    HALF_KILL_RATE,

    MP_CONSUME,
    MP_INITIAL_CONSUME,
    HP_CONSUME,
    CONSUMED_ITEM,
    SOUL_CONSUME,
    ENERGY_CONSUME,
    CHARGE_CONSUME,

    CAST_RANGE,
    EFFECT_RANGE,
    AOE_RANGE,
    MAX_TARGETS,
    FAN_START_ANGLE,
    FAN_RADIUS,
    FAN_ANGLE,

    CAST_TIME,
    COOL_TIME,
    REUSE_DELAY,

    ABNORMAL_LEVEL,
    ABNORMAL_TIME,
    ABNORMAL_TYPE,

    ACTIVATE_RATE,
    MIN_CHANCE,
    MAX_CHANCE,
    LEVEL_MODIFIER,
    SAVE_VS,

    NEGATE_RATE,

    AGGRO_POINTS,

    TARGET,
    OPERATION,
    MAGIC,
    OFFENSIVE,
    OVER_HIT,
    OLYMPIAD_USABLE,
    ENCHANTABLE,
    REQUIRED_WEAPON
}
