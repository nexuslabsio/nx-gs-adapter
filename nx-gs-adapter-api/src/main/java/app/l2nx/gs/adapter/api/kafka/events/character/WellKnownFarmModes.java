package app.l2nx.gs.adapter.api.kafka.events.character;

/**
 * Canonical values for the {@code farm_mode} metadata key of
 * {@link CharacterDeathEvent} (see {@link WellKnownDeathMetadata#FARM_MODE}) —
 * the unattended mode a character was in when it died. The value is an
 * <b>open string</b> so a core with a different set of unattended modes is not a
 * breaking contract change.
 *
 * <p>Mirrors the {@code WellKnown*} pattern on the other event DTOs
 * ({@link WellKnownKillerTypes}, {@code WellKnownSiegeOutcomes}). The set is
 * non-exhaustive: a host MAY emit a non-canonical mode string without an API
 * release, and consumers treat unknown values as opaque. Adding a constant here
 * is a non-breaking minor-version change. Values are {@code lower_snake_case}.</p>
 *
 * <ul>
 *   <li>{@link #AUTOFARM} — the character was on the server-side auto-hunt bot.</li>
 *   <li>{@link #AUTO_MACRO} — the character was leveling on a server-managed
 *   auto-macro (official cycle-macro session).</li>
 * </ul>
 */
public final class WellKnownFarmModes {

    private WellKnownFarmModes() {
    }

    public static final String AUTOFARM = "autofarm";

    public static final String AUTO_MACRO = "auto_macro";
}
