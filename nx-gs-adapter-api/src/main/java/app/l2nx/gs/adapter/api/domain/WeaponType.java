package app.l2nx.gs.adapter.api.domain;

/**
 * Canonical, build-agnostic weapon-type vocabulary, shared across item ({@code weapon_type})
 * and skill ({@code REQUIRED_WEAPON} — the weapons a skill may be cast with). A cross-entity
 * domain enum (not tied to one slice or wire DTO); a producer maps its core's weapon-type
 * representation (an enum, a mask, or a datapack {@code <using kind="...">} display name)
 * onto these tokens.
 *
 * <p>SCREAMING_SNAKE canonical names — distinct from raw source forms ({@code DUALDAGGER}
 * in datapack columns, {@code "Dual Dagger"} in client conditions both map to
 * {@link #DUAL_DAGGER}). The full set mirrors the L2 weapon kinds; the current server's
 * item data exercises 19 of them ({@link #TWO_HAND_CROSSBOW} and {@link #DUAL_BLUNT} are
 * absent there but kept for completeness).</p>
 */
public enum WeaponType {
    SWORD,
    BIG_SWORD,
    ANCIENT_SWORD,
    DUAL_SWORD,
    BLUNT,
    BIG_BLUNT,
    DUAL_BLUNT,
    DAGGER,
    DUAL_DAGGER,
    FIST,
    DUAL_FIST,
    BOW,
    CROSSBOW,
    TWO_HAND_CROSSBOW,
    POLE,
    RAPIER,
    FISHING_ROD,
    FLAG,
    OWN_THING,
    ETC,
    NONE
}
