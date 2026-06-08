package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Canonical, build-agnostic skill-characteristic vocabulary — the closed set of keys that
 * may appear in a skill's typed-characteristics ({@code stats}) bag. A shared skill-domain
 * enum (not tied to one wire DTO): the wiki assembles the bag read-side from structural
 * {@code gd_*} columns, keying each entry by {@code name()} (open string, consistent with
 * the platform's enum-like-vocab convention). This enum is the source of truth for
 * producing and documenting those keys.
 *
 * <p>Covers the level-invariant header characteristics ({@link #TARGET}, {@link #OPERATION},
 * the boolean flags, {@link #ENCHANTABLE}, {@link #REQUIRED_WEAPON}) and the per-level ones
 * (consume / range / timing / abnormal / crit). The value type of each key is fixed by the
 * read-side mapper (see {@code StatType}): e.g. {@link #TARGET} is an {@code ENUM} token,
 * {@link #REUSE_DELAY} a {@code DURATION}, {@link #REQUIRED_WEAPON} an {@code ARRAY},
 * {@link #CONSUMED_ITEM} an item-template-and-count.</p>
 */
public enum SkillStat {

    // Magnitude
    POWER,
    MAGIC_LEVEL,
    CRIT_RATE,
    BLOW_CHANCE,

    // Consumption
    MP_CONSUME,
    MP_INITIAL_CONSUME,
    HP_CONSUME,
    CONSUMED_ITEM,

    // Range / area
    CAST_RANGE,
    EFFECT_RANGE,
    AOE_RANGE,
    MAX_TARGETS,

    // Timing
    CAST_TIME,
    COOL_TIME,
    REUSE_DELAY,

    // Abnormal (buff/debuff)
    ABNORMAL_LEVEL,
    ABNORMAL_TIME,

    // Classification
    TARGET,
    OPERATION,
    MAGIC,
    OFFENSIVE,
    OVER_HIT,
    OLYMPIAD_USABLE,
    ENCHANTABLE,
    REQUIRED_WEAPON
}
