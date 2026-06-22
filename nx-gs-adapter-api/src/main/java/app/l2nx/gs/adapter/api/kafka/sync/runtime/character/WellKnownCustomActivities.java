package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

/**
 * Canonical {@code type} discriminator values for {@link CustomActivity#getType()}
 * (one entry of {@link CharacterRuntimeDto#getCustomActivities()}). The
 * custom activity is a <b>build-specific</b>, high-level "what the player is
 * occupied with" signal that lives outside the engine AI state machine —
 * sustained activities a particular core implements on its own state and timers
 * rather than through {@code CtrlIntention}. Activity-specific extras (elapsed
 * time, penalty tier, …) ride the {@code metadata} map — see
 * {@link WellKnownCustomActivityMetadata}.
 *
 * <p>The classic example is fishing: it is a self-contained mini-engine on most
 * cores (its own immobilize + tick loop, no AI intention), and some servers do
 * not ship it at all — replacing it with a different sustained activity (reading
 * a book, mining, etc.). Because the activity set genuinely varies per build,
 * the {@code type} is an <b>open string</b> with the widest possible host
 * freedom: a host emits whatever lower_snake_case activity key its core supports,
 * and consumers map known values to a label / icon while falling back to the raw
 * string for unknowns. A {@code null} / empty {@code customActivities} means "no
 * special activity".</p>
 *
 * <p>The constants below are merely the activities seen often enough to be worth
 * naming — they are NOT an exhaustive or required set, and a build that has none
 * of them is perfectly valid. Adding a new constant here is a non-breaking
 * minor-version change in {@code nx-gs-adapter-api}; a host does NOT need an API
 * release to emit a brand-new activity string.</p>
 *
 * <p>Independent of {@link WellKnownAiStatuses aiStatus} — the two fields are
 * orthogonal and carry no precedence between them on the wire.</p>
 *
 * <ul>
 *     <li>{@link #FISHING} — the character is fishing.</li>
 *     <li>{@link #READING} — the character is reading a book (custom activity on
 *     cores that ship it in place of, or alongside, fishing).</li>
 *     <li>{@link #AUTOFARMING} — the character is auto-farming (server-side
 *     bot/auto-hunt). Time-limited builds ride a {@code seconds_remaining}
 *     metadata key — see {@link WellKnownCustomActivityMetadata}.</li>
 *     <li>{@link #AUTO_MACRO} — the character is leveling on a server-managed
 *     auto-macro (official cycle-macro session, distinct from {@link #AUTOFARMING}).
 *     Carries {@code elapsed_seconds} and, on quota-limited builds,
 *     {@code seconds_remaining} — see {@link WellKnownCustomActivityMetadata}.</li>
 * </ul>
 */
public final class WellKnownCustomActivities {

    private WellKnownCustomActivities() {}

    /**
     * The character is fishing.
     */
    public static final String FISHING = "fishing";

    /**
     * The character is reading a book.
     */
    public static final String READING = "reading";

    /**
     * The character is auto-farming (server-side auto-hunt). On time-limited
     * builds the remaining auto-farm time rides the
     * {@link WellKnownCustomActivityMetadata#SECONDS_REMAINING} metadata key
     * (absent when the farm is unlimited / free).
     */
    public static final String AUTOFARMING = "autofarming";

    /**
     * The character is leveling on a server-managed auto-macro (official
     * cycle-macro session) — distinct from {@link #AUTOFARMING}, which is the
     * server-side auto-hunt bot. On quota-limited builds the remaining macro
     * time rides {@link WellKnownCustomActivityMetadata#SECONDS_REMAINING}
     * (absent when the macro is unlimited); elapsed time rides
     * {@link WellKnownCustomActivityMetadata#ELAPSED_SECONDS}.
     */
    public static final String AUTO_MACRO = "auto_macro";
}
