package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

/**
 * Canonical values for {@link CharacterRuntimeDto#getAiStatus()}. The AI status
 * is the engine-native control intention of a player character — the reactive
 * server-side state machine that translates client input (move / attack / cast /
 * pick-up / interact) into world actions. Nearly every L2 core (L2J, Lucera,
 * Essence and forks) models this as a small, stable intention enum, so the
 * canonical set below is broad across builds.
 *
 * <p>The field is an <b>open string</b>: a host adapter maps its own intention
 * vocabulary onto these canonical lower_snake_case values
 * ({@code CtrlIntention.PICK_UP} → {@code pick_up}, etc.). Hosts MAY emit
 * additional non-canonical statuses for build-specific intentions; consumers
 * treat unknown values as opaque (display the raw string, no behaviour routed on
 * it). Adding a new constant here is a non-breaking minor-version change in
 * {@code nx-gs-adapter-api}.</p>
 *
 * <p>Independent of {@link WellKnownCustomActivities customActivity} — the two
 * fields are orthogonal. A fishing character, for example, is typically
 * {@link #IDLE} on the AI axis while {@code customActivity=fishing}. No
 * precedence between the two is implied by the wire contract; consumers decide
 * how (or whether) to combine them.</p>
 *
 * <ul>
 *     <li>{@link #IDLE} — full stop, awaiting input.</li>
 *     <li>{@link #ACTIVE} — passive ready state, reacting to events.</li>
 *     <li>{@link #REST} — sitting / resting.</li>
 *     <li>{@link #ATTACK} — engaging a melee / auto-attack target.</li>
 *     <li>{@link #CAST} — casting a skill / spell.</li>
 *     <li>{@link #MOVING} — walking to a destination.</li>
 *     <li>{@link #FOLLOW} — following a target.</li>
 *     <li>{@link #PICK_UP} — moving to pick up a ground item.</li>
 *     <li>{@link #INTERACT} — interacting with an NPC / door / object.</li>
 * </ul>
 */
public final class WellKnownAiStatuses {

    private WellKnownAiStatuses() {
    }

    /**
     * Full stop, awaiting client input.
     */
    public static final String IDLE = "idle";

    /**
     * Passive ready state, reacting to events.
     */
    public static final String ACTIVE = "active";

    /**
     * Sitting / resting.
     */
    public static final String REST = "rest";

    /**
     * Engaging a melee / auto-attack target.
     */
    public static final String ATTACK = "attack";

    /**
     * Casting a skill / spell.
     */
    public static final String CAST = "cast";

    /**
     * Walking to a destination.
     */
    public static final String MOVING = "moving";

    /**
     * Following a target.
     */
    public static final String FOLLOW = "follow";

    /**
     * Moving to pick up a ground item.
     */
    public static final String PICK_UP = "pick_up";

    /**
     * Interacting with an NPC / door / object.
     */
    public static final String INTERACT = "interact";
}
