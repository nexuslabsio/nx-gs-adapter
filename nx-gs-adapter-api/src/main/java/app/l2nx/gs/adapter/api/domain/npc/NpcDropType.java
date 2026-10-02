package app.l2nx.gs.adapter.api.domain.npc;

/**
 * Category of an NPC reward group.
 *
 * <ul>
 *     <li>{@link #DROP} - ungrouped, non-rated single drops.</li>
 *     <li>{@link #RATED_GROUPED} - one group is chosen by {@code groupChance}, then items roll individually.</li>
 *     <li>{@link #UNGROUPED} - non-rated grouped drops.</li>
 *     <li>{@link #SWEEP} - spoil drops.</li>
 * </ul>
 */
public enum NpcDropType {
    DROP,
    RATED_GROUPED,
    UNGROUPED,
    SWEEP
}
