package app.l2nx.gs.adapter.api.kafka.events.raid.model;

/**
 * Coarse boss classification, a shared vocabulary only: the host decides which value applies, the adapter
 * and platform infer nothing from it. Finer taxonomy is consumer-side, derived from {@code bossNpcId}.
 */
public enum RaidBossKind {
    RAID,

    /** Which bosses qualify is decided by the host. */
    EPIC,

    INSTANCE_BOSS,
}
