package app.l2nx.gs.adapter.api.kafka.events.privatestore;

/**
 * Canonical key constants for the {@code elementalAttrs} maps on
 * {@link TradeLine#getElementalAttrs()} and {@link Offer#getElementalAttrs()}.
 * Hosts MAY use additional non-canonical keys — the platform treats unknown
 * keys as opaque strings — but populating these constants when applicable lets
 * cross-tenant dashboards aggregate the same elemental attribute consistently.
 *
 * <p>Catalog covers the six standard L2 elemental attributes present from
 * Hellbound onwards across L2J / Lucera / Essence forks. Adding a new constant
 * is a non-breaking minor-version change in {@code nx-gs-adapter-api}.</p>
 *
 * <p>Values are stored as the elemental attribute power in points
 * ({@code Integer}) — the raw number shown on the item tooltip
 * (e.g. {@code 300} for {@code +300 Fire Attack}). Hosts that surface attack
 * and defense attributes separately should publish only the dominant one for
 * the slot the item occupies (weapon → attack, armor → defense).</p>
 */
public final class WellKnownElements {

    private WellKnownElements() {
    }

    /**
     * Fire elemental attribute.
     */
    public static final String FIRE = "fire";

    /**
     * Water elemental attribute.
     */
    public static final String WATER = "water";

    /**
     * Earth elemental attribute.
     */
    public static final String EARTH = "earth";

    /**
     * Wind elemental attribute.
     */
    public static final String WIND = "wind";

    /**
     * Holy elemental attribute.
     */
    public static final String HOLY = "holy";

    /**
     * Dark / unholy elemental attribute.
     */
    public static final String DARK = "dark";
}
