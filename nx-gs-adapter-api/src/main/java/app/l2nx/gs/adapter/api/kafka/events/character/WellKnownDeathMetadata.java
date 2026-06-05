package app.l2nx.gs.adapter.api.kafka.events.character;

/**
 * Canonical key constants for the {@code metadata} map of
 * {@link CharacterDeathEvent}. The map is open — hosts MAY publish arbitrary
 * additional keys; consumers treat unknown keys as opaque. Adding a constant
 * here is a non-breaking minor-version change. Mirrors the {@code WellKnown*}
 * pattern on the other event DTOs.
 *
 * <ul>
 *   <li>{@link #KILLER_TYPE} — a {@link WellKnownKillerTypes} value classifying
 *   the killer ({@code monster} / {@code player} / {@code boss} / {@code self}).
 *   Absent when the host does not classify.</li>
 *   <li>{@link #KILLER_ID} — the killer's id as a decimal string: the killer's
 *   <b>character object-id</b> when {@code killer_type=player}, or the killer's
 *   <b>NPC template-id</b> when {@code killer_type=monster}/{@code boss}. Absent
 *   for {@code self} / unattributable deaths. The platform resolves the killer's
 *   display name from this id against its character / NPC catalogs — no killer
 *   name is carried on the wire.</li>
 *   <li>{@link #FARM_MODE} — the unattended mode the character was in at death
 *   (a {@link WellKnownFarmModes} value — {@code autofarm} / {@code auto_macro}).
 *   Present only on the unattended-death signal bohpts emits; absent when the
 *   host does not classify the mode.</li>
 * </ul>
 */
public final class WellKnownDeathMetadata {

    private WellKnownDeathMetadata() {
    }

    public static final String KILLER_TYPE = "killer_type";

    public static final String KILLER_ID = "killer_id";

    /**
     * The unattended mode the character was in at death — a
     * {@link WellKnownFarmModes} value. Lets the platform word the
     * "your unattended character died" notification per mode. Absent when the
     * host does not classify.
     */
    public static final String FARM_MODE = "farm_mode";
}
