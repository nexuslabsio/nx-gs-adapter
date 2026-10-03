package app.l2nx.gs.adapter.api.kafka.events.raid.model;

/**
 * Coarse boss classification, a shared vocabulary only: the host decides which value applies, the adapter
 * and platform infer nothing from it. Finer taxonomy is consumer-side, derived from {@code bossNpcId}.
 *
 * @deprecated the platform no longer reads it; boss classification is {@code NpcTemplate.type}
 * ({@code WellKnownNpcTypes}). Still sent on the wire so older and newer hosts keep working.
 */
@Deprecated
// TODO: remove once all hosts run the adapter that ships WellKnownNpcTypes and the raid topic is drained
public enum RaidBossKind {
    RAID,

    /** Which bosses qualify is decided by the host. */
    EPIC,

    INSTANCE_BOSS,
}
