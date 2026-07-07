package app.l2nx.gs.adapter.api.domain.stat;

/**
 * Unified, build-agnostic stat vocabulary — the closed set of keys that may appear in any
 * {@code Map<String, Double>} stats bag on the wire (items, NPCs, skills). Emitted as
 * {@code name()} (UPPER_SNAKE), consistent with the platform's enum-like-vocab convention.
 *
 * <p>This is a type consolidation of the former per-entity enums ({@code ItemStat},
 * {@code NpcStat}, {@code SkillStat}) — token names are verbatim, so JSONB keys in
 * {@code gd_*} tables are unchanged. Not every token applies to every entity; applicability
 * is documented per-DTO via {@code @Schema(allowableValues = ...)}.</p>
 *
 * <p>Intentional non-harmonised coexistence:
 * {@code AGGRO_POINTS} (skill-produced threat) and {@code AGGRO_RANGE} (NPC aggro radius) are
 * different dimensions; {@code SPEED} (item movement bonus) coexists with {@code RUN_SPEED} /
 * {@code WALK_SPEED} (NPC absolute speeds) — all three kept verbatim to preserve item/NPC
 * wire keys.</p>
 */
public enum Stat {

    // Combat — offense
    P_ATK,
    M_ATK,
    ATK_SPD,
    CAST_SPD,
    CRIT_RATE,
    M_CRIT_RATE,
    ACCURACY,
    ATK_RANGE,
    ATK_ANGLE,

    // Combat — defense
    P_DEF,
    M_DEF,
    EVASION,
    SHIELD_DEF,
    SHIELD_RATE,
    M_SUCCESS_RES,

    // Vitals & regen
    MAX_HP,
    MAX_MP,
    HP_REGEN,
    MP_REGEN,

    // Movement
    /**
     * Item movement speed bonus.
     */
    SPEED,
    /**
     * NPC absolute run speed.
     */
    RUN_SPEED,
    /**
     * NPC absolute walk speed.
     */
    WALK_SPEED,

    // Weapon mechanics (items only)
    RANDOM_DAMAGE,
    SOULSHOT_COUNT,
    SPIRITSHOT_COUNT,
    MAGIC_WEAPON,

    // Special / non-combat (items only)
    AUTOLOOT,
    INV_LIMIT,

    // Base stats
    STR,
    DEX,
    CON,
    INT,
    WIT,
    MEN,

    // Attribute attack power
    FIRE_POWER,
    WATER_POWER,
    WIND_POWER,
    EARTH_POWER,
    HOLY_POWER,
    DARK_POWER,

    // Attribute resistance
    FIRE_RES,
    WATER_RES,
    WIND_RES,
    EARTH_RES,
    HOLY_RES,
    DARK_RES,

    // NPC behaviour
    /**
     * NPC aggro detection radius.
     */
    AGGRO_RANGE,

    // Skill magnitude
    POWER,
    PVP_POWER,
    PVE_POWER,
    MAGIC_LEVEL,
    BLOW_CHANCE,
    LETHAL_STRIKE_RATE,
    HALF_KILL_RATE,

    // Skill consumption
    MP_CONSUME,
    MP_INITIAL_CONSUME,
    HP_CONSUME,
    CONSUMED_ITEM,
    SOUL_CONSUME,
    ENERGY_CONSUME,
    CHARGE_CONSUME,

    // Skill range / area
    CAST_RANGE,
    EFFECT_RANGE,
    AOE_RANGE,
    MAX_TARGETS,
    FAN_START_ANGLE,
    FAN_RADIUS,
    FAN_ANGLE,

    // Skill timing
    CAST_TIME,
    COOL_TIME,
    REUSE_DELAY,

    // Skill abnormal (buff/debuff)
    ABNORMAL_LEVEL,
    ABNORMAL_TIME,
    ABNORMAL_TYPE,

    // Skill land rate
    ACTIVATE_RATE,
    MIN_CHANCE,
    MAX_CHANCE,
    LEVEL_MODIFIER,
    SAVE_VS,

    // Skill negate (cleanse)
    NEGATE_RATE,

    // Skill threat
    /**
     * Skill-produced threat / aggro points.
     */
    AGGRO_POINTS,

    // Skill classification
    TARGET,
    OPERATION,
    MAGIC,
    OFFENSIVE,
    OVER_HIT,
    OLYMPIAD_USABLE,
    ENCHANTABLE,
    REQUIRED_WEAPON
}
