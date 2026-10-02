package app.l2nx.gs.adapter.api.domain;

/**
 * Weapon kind shared by item ({@code weapon_type}) and skill ({@code REQUIRED_WEAPON}). Raw source
 * forms ({@code DUALDAGGER}, {@code "Dual Dagger"}) both map to {@link #DUAL_DAGGER}.
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
