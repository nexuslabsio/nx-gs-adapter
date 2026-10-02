package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Combat trait a skill belongs to (vulnerability / proficiency category); one per skill.
 * {@link #NONE} means not subject to trait resistances.
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
