package app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.model;

import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;

/**
 * Canonical values for {@code ItemTemplate#getGearScoreEnchantProfile()}, an open string so builds can ship their own;
 * unknown profiles are stored verbatim. Each value is the {@code key} of the matching {@code ENCHANT_PROFILE} rule in
 * {@link GearScoreRuleset}. {@link #SPECIAL} covers cloaks, one-piece and custom items.
 */
public final class WellKnownGearScoreEnchantProfiles {

    private WellKnownGearScoreEnchantProfiles() {}

    public static final String WEAPON = "WEAPON";
    public static final String NONWEAPON = "NONWEAPON";
    public static final String SPECIAL = "SPECIAL";
}
