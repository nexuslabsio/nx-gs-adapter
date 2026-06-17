package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore;

/**
 * Canonical values for
 * {@code ItemTemplate#getGearScoreEnchantProfile()}. The field is an
 * <b>open string</b> so a build shipping its own profiles is not a breaking
 * contract change. Mirrors the {@code WellKnown*} pattern on the other DTOs; the
 * set is non-exhaustive and the platform stores unknown profiles verbatim. Adding
 * a constant is a non-breaking minor-version change. Values are
 * {@code UPPER_SNAKE_CASE}.
 *
 * <p>The profile key references the matching rule in the {@code ENCHANT_PROFILE}
 * group of {@link GearScoreRuleset} — the rule whose {@code key} equals this value
 * — so a consumer can resolve the per-enchant-level gear-score scaling for an item.
 * A build with no named-profile concept leaves the field {@code null}.
 *
 * <ul>
 *   <li>{@link #WEAPON} — the default weapon profile.</li>
 *   <li>{@link #NONWEAPON} — the default armor / jewelry profile.</li>
 *   <li>{@link #SPECIAL} — special items (cloaks, one-piece, custom).</li>
 * </ul>
 */
public final class WellKnownGearScoreEnchantProfiles {

    private WellKnownGearScoreEnchantProfiles() {
    }

    public static final String WEAPON = "WEAPON";
    public static final String NONWEAPON = "NONWEAPON";
    public static final String SPECIAL = "SPECIAL";
}
