package app.l2nx.gs.adapter.api.kafka.events.castle;

/**
 * Canonical {@link SiegeFinishedEvent#getOutcome()} values. The field is an open string; consumers treat unknown
 * values as opaque. {@code captured}: another clan took the castle; {@code defended}: the prior owner held it;
 * {@code draw}: unowned at siege end ({@code winnerClanId} is {@code null}).
 */
public final class WellKnownSiegeOutcomes {

    private WellKnownSiegeOutcomes() {}

    public static final String CAPTURED = "captured";

    public static final String DEFENDED = "defended";

    public static final String DRAW = "draw";
}
