package app.l2nx.gs.adapter.api.domain.skill;

/**
 * Canonical, build-agnostic skill-activation kind — the three-way split of how a skill
 * operates. A shared skill-domain enum (not tied to one wire DTO). It is the readable
 * fold of a core's finer-grained operate-type code (e.g. an L2J/L2E {@code A1…A6}/{@code CA*}/
 * {@code DA*} → {@link #ACTIVE}, {@code P} → {@link #PASSIVE}, {@code T}/{@code TG}/{@code AU}
 * → {@link #TOGGLE}); the wiki derives it read-side from the skill's {@code passive} /
 * {@code toggle} flags, while the raw operate-type stays a structural filter field.
 */
public enum SkillOperation {
    ACTIVE,
    PASSIVE,
    TOGGLE
}
