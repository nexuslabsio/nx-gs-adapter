package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Status-bar group of a skill, not of one application. Precedence when several flags hold: {@link #DEBUFF},
 * {@link #TRIGGERED}, {@link #SONG_DANCE}, {@link #TOGGLE}, otherwise {@link #BUFF}.
 */
public enum SkillEffectCategory {
    BUFF,
    DEBUFF,
    SONG_DANCE,
    TOGGLE,
    TRIGGERED
}
