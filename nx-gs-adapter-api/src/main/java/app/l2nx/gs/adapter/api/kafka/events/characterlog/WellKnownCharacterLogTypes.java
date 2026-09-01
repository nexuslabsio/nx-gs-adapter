package app.l2nx.gs.adapter.api.kafka.events.characterlog;

/**
 * Canonical values for {@link CharacterLogEvent#getType()}. The set is open — hosts MAY publish other
 * tokens, and consumers keep unknown ones rather than dropping them.
 *
 * <p>Profession tokens name the tier reached, which is the host's {@code ClassId.level()} after the
 * change: base classes sit at 0, so {@code FIRST_CLASS} is level 1. A profession taken while a
 * subclass is active carries {@code class_index > 0} — reported either way, and whether it counts is
 * the consumer's decision.</p>
 */
public final class WellKnownCharacterLogTypes {

    private WellKnownCharacterLogTypes() {}

    /** First profession taken (class tier 1). */
    public static final String FIRST_CLASS = "FIRST_CLASS";

    /** Second profession taken (class tier 2). */
    public static final String SECOND_CLASS = "SECOND_CLASS";

    /** Third profession taken (class tier 3). */
    public static final String THIRD_CLASS = "THIRD_CLASS";

    /** A subclass was added to the character. Not emitted on subclass switching. */
    public static final String SUBCLASS_ADDED = "SUBCLASS_ADDED";

    /** Noblesse status obtained. Emitted on the transition only, never on revocation. */
    public static final String NOBLESSE = "NOBLESSE";
}
