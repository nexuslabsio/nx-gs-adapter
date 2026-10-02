package app.l2nx.gs.adapter.api.domain.skill;

/**
 * How a skill operates, folded from the core's operate-type code ({@code A*}/{@code CA*}/{@code DA*}
 * to {@link #ACTIVE}, {@code P} to {@link #PASSIVE}, {@code T}/{@code TG}/{@code AU} to {@link #TOGGLE}).
 */
public enum SkillOperation {
    ACTIVE,
    PASSIVE,
    TOGGLE
}
