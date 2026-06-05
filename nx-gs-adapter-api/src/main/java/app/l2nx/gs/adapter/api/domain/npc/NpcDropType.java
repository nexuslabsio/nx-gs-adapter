package app.l2nx.gs.adapter.api.domain.npc;

/**
 * Category of an NPC reward group — the canonical, build-agnostic drop-type
 * vocabulary. A provider maps its core's internal reward-list classification onto
 * these.
 *
 * <ul>
 *     <li>{@link #DROP} — ungrouped, non-rated single drops.</li>
 *     <li>{@link #RATED_GROUPED} — rate-affected grouped drops (one group is
 *     chosen by {@code groupChance}, then items roll individually).</li>
 *     <li>{@link #UNGROUPED} — non-rated grouped drops.</li>
 *     <li>{@link #SWEEP} — spoil drops (harvested with the Sweeper skill).</li>
 * </ul>
 */
public enum NpcDropType {
    DROP,
    RATED_GROUPED,
    UNGROUPED,
    SWEEP
}
