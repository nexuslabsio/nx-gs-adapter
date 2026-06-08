package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Canonical, build-agnostic skill-trait vocabulary — the combat trait a skill belongs to
 * (the category against which targets compute vulnerability and casters compute
 * proficiency). A shared skill-domain enum (not tied to one wire DTO); a host provider
 * emits its core trait name, which maps 1:1 onto these constants. One trait per skill.
 *
 * <ul>
 *   <li>{@link #NONE} — no trait (not subject to trait resistances).</li>
 *   <li>{@link #BLEED} — bleeding (physical damage-over-time).</li>
 *   <li>{@link #BOSS} — boss-class resistance trait.</li>
 *   <li>{@link #DEATH} — instant-death effects.</li>
 *   <li>{@link #DERANGEMENT} — mental debuff (confusion family).</li>
 *   <li>{@link #ETC} — uncategorized / other.</li>
 *   <li>{@link #GUST} — knockback / wind.</li>
 *   <li>{@link #HOLD} — root / immobilize.</li>
 *   <li>{@link #PARALYZE} — paralysis (full immobilize + action block).</li>
 *   <li>{@link #PHYSICAL_BLOCKADE} — physical block.</li>
 *   <li>{@link #POISON} — poison damage-over-time.</li>
 *   <li>{@link #SHOCK} — stun family.</li>
 *   <li>{@link #SLEEP} — sleep (breaks on damage).</li>
 *   <li>{@link #VALAKAS} — Valakas raid-specific resistance trait.</li>
 *   <li>{@link #DISARM_WEAPON} — weapon disarm.</li>
 * </ul>
 */
public enum SkillTrait {
    NONE,
    BLEED,
    BOSS,
    DEATH,
    DERANGEMENT,
    ETC,
    GUST,
    HOLD,
    PARALYZE,
    PHYSICAL_BLOCKADE,
    POISON,
    SHOCK,
    SLEEP,
    VALAKAS,
    DISARM_WEAPON
}
