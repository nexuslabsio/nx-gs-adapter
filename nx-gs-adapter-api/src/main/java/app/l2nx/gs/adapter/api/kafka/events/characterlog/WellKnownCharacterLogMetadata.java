package app.l2nx.gs.adapter.api.kafka.events.characterlog;

/**
 * Canonical keys for the {@code metadata} map of {@link CharacterLogEvent}. The map is open and every
 * value is a decimal string.
 *
 * <p>Which keys a {@code type} carries is an expectation, not a wire constraint — a fact missing one
 * is still valid, and the consumer decides whether it can act on it.</p>
 */
public final class WellKnownCharacterLogMetadata {

    private WellKnownCharacterLogMetadata() {}

    /** Class held after the fact; on {@code SUBCLASS_ADDED}, the added subclass. */
    public static final String CLASS_ID = "class_id";

    /** Tier reached, {@code 1} / {@code 2} / {@code 3}. Redundant with the type token by design —
     * it survives a consumer that does not recognise the token. */
    public static final String CLASS_LEVEL = "class_level";

    /**
     * Slot the change applied to — {@code 0} is the main class. Absent means the host does not track
     * slots; a main-class-only consumer SHOULD skip rather than assume {@code 0}.
     */
    public static final String CLASS_INDEX = "class_index";

    /** Slot the subclass was added into. Present on {@code SUBCLASS_ADDED}. */
    public static final String SUBCLASS_INDEX = "subclass_index";

    /** Character level at the moment of the fact. Optional on every type. */
    public static final String CHAR_LEVEL = "char_level";
}
