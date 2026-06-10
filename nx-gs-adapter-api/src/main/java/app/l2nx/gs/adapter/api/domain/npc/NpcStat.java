package app.l2nx.gs.adapter.api.domain.npc;

/**
 * Canonical, build-agnostic npc-stat vocabulary — the closed set of keys that may appear
 * in {@code NpcTemplate.getStats()}. A shared npc-domain enum (not tied to one wire DTO):
 * the provider maps its core's template fields onto these tokens so the wire is
 * build-agnostic, and consumers (storage, read APIs, the frontend) drive typed UI /
 * translations off the same closed set.
 *
 * <p>The wire itself carries {@code Map<String,Double>} keyed by {@code name()} (open
 * string, consistent with the platform's enum-like-vocab convention) — this enum is the
 * source of truth for producing and documenting those keys. Zero values are dropped by
 * the producer ("not applicable"). Token names are shared with {@code ItemStat} wherever
 * the stat dimension matches ({@code MAX_HP}, {@code P_ATK}, {@code CAST_SPD}, …);
 * movement speeds spell {@code SPEED} in full while attack/cast speeds abbreviate to
 * {@code _SPD} — both mirroring the item vocabulary. The attack type is not a magnitude
 * and rides outside the map ({@code NpcTemplate.getAtkType()}, a {@code WeaponType}
 * token).</p>
 */
public enum NpcStat {

    // Vitals & regen
    MAX_HP,
    MAX_MP,
    HP_REGEN,
    MP_REGEN,

    // Offense
    P_ATK,
    M_ATK,
    ATK_SPD,
    CAST_SPD,
    CRIT_RATE,
    ATK_RANGE,

    // Defense
    P_DEF,
    M_DEF,
    SHIELD_DEF,
    SHIELD_RATE,

    // Movement
    RUN_SPEED,
    WALK_SPEED,

    // Behaviour
    AGGRO_RANGE,

    // Base stats
    STR,
    DEX,
    CON,
    INT,
    WIT,
    MEN,

    // Elemental attack power
    FIRE_POWER,
    WATER_POWER,
    WIND_POWER,
    EARTH_POWER,
    HOLY_POWER,
    DARK_POWER,

    // Elemental resistance
    FIRE_RES,
    WATER_RES,
    WIND_RES,
    EARTH_RES,
    HOLY_RES,
    DARK_RES
}
