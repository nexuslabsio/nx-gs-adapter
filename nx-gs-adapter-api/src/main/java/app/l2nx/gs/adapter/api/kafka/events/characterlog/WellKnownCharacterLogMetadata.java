package app.l2nx.gs.adapter.api.kafka.events.characterlog;

/**
 * Canonical key constants for the {@code metadata} map of
 * {@link CharacterLogEvent}. The map is open — hosts MAY publish arbitrary
 * additional keys; consumers treat unknown keys as opaque. Every value is a
 * decimal string, matching the other metadata maps in this API.
 *
 * <p>Which keys a given {@code type} carries is a per-type expectation
 * documented on the constants below, not a wire-level constraint: a fact missing
 * one is still a valid event, and the consumer decides whether it can act on it.
 * The character's identity is not repeated here — {@code charId} is a top-level
 * field, and the platform resolves the owning account from its own character
 * catalog.</p>
 */
public final class WellKnownCharacterLogMetadata {

    private WellKnownCharacterLogMetadata() {}

    /**
     * The L2 class id the character holds after the fact. Present on the
     * class-transfer types and on {@code SUBCLASS_ADDED} (the added subclass).
     */
    public static final String CLASS_ID = "class_id";

    /**
     * Profession tier reached — {@code 1} / {@code 2} / {@code 3}, the host's
     * {@code ClassId.level()} after the transfer. Present on the class-transfer
     * types. Redundant with the type token by design: it survives a consumer
     * that does not recognise the token.
     */
    public static final String CLASS_LEVEL = "class_level";

    /**
     * Class slot the transfer applied to — {@code 0} for the main class, greater
     * for a subclass slot. Absent means the host does not track slots; consumers
     * that only care about the main class SHOULD treat an absent value as
     * {@code 0} only when they know the host, and otherwise skip.
     */
    public static final String CLASS_INDEX = "class_index";

    /** Slot the subclass was added into. Present on {@code SUBCLASS_ADDED}. */
    public static final String SUBCLASS_INDEX = "subclass_index";

    /** Character level at the moment of the fact. Optional on every type. */
    public static final String CHAR_LEVEL = "char_level";
}
