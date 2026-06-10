package app.l2nx.gs.adapter.api.domain.item;

/**
 * Canonical, build-agnostic item-stat vocabulary — the closed set of keys that may
 * appear in {@code ItemStats.getStats()}. A shared item-domain enum (not tied to one
 * wire DTO): the provider maps its core's build-specific stat representation (e.g. an
 * L2J {@code Stats} enum) onto these tokens so the wire is build-agnostic, and consumers
 * (storage, read APIs, the frontend) drive typed UI / translations off the same closed
 * set.
 *
 * <p>The wire itself carries {@code Map<String,Double>} keyed by {@code name()} (open
 * string, consistent with the platform's enum-like-vocab convention) — this enum is the
 * source of truth for producing and documenting those keys, not a hard wire-key type.
 * A stat the provider cannot map to one of these tokens is dropped rather than emitted
 * raw, keeping the vocabulary closed. Adding a stat = one constant here + one mapping
 * entry in the provider.
 */
public enum ItemStat {

    // Offense
    P_ATK,
    M_ATK,
    ATK_SPD,
    CAST_SPD,
    CRIT_RATE,
    M_CRIT_RATE,
    ACCURACY,
    ATK_RANGE,
    ATK_ANGLE,

    // Defense
    P_DEF,
    M_DEF,
    EVASION,
    SHIELD_DEF,
    SHIELD_RATE,
    M_SUCCESS_RES,

    // Vitals & movement
    MAX_HP,
    MAX_MP,
    SPEED,

    // Weapon mechanics (sourced from weapon accessors, not stat-funcs)
    RANDOM_DAMAGE,
    SOULSHOT_COUNT,
    SPIRITSHOT_COUNT,
    MP_CONSUME,

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

    // Special / non-combat
    AUTOLOOT,
    INV_LIMIT,

    // Base stats
    STR,
    DEX,
    CON,
    INT,
    WIT,
    MEN
}
