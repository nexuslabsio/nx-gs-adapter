package app.l2nx.gs.adapter.api.kafka.events.gameevents;

/**
 * Keys of the open {@code metadata} map of {@link GameEventEntry}; consumers ignore unknown keys. Absent when the
 * host does not classify an event.
 */
public final class WellKnownGameEventMetadata {

    private WellKnownGameEventMetadata() {}

    public static final String EVENT_KIND = "event_kind";

    /**
     * Team-vs-team style mass-PvP event; e.g. a host may map it from its own team-vs-team or solo PvP zone events.
     */
    public static final String EVENT_KIND_TVT = "tvt";

    /**
     * Scheduled solo / free-for-all boss-hunt event, not a respawning raid boss.
     */
    public static final String EVENT_KIND_SOLO_BOSS = "solo_boss";
}
