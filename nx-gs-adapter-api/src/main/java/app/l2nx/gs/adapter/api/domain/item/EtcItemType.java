package app.l2nx.gs.adapter.api.domain.item;

/**
 * Canonical, build-agnostic etc-item-type vocabulary — the {@code etc_item_type} of an
 * {@link ItemClass#ETC} item. A shared item-domain enum (not tied to one wire DTO); a provider
 * maps its core's internal etc-item-type representation onto these.
 *
 * <p>SCREAMING_SNAKE canonical names. The enchant-scroll family is spelled {@code SCROLL_*} in
 * full (no {@code SCRL} abbreviation, consistent with the standalone {@link #SCROLL}); the
 * {@code _AM} / {@code _WP} suffixes distinguish armor / weapon enchant scrolls. The set mirrors
 * the etc-item categories the platform actually surfaces; a source type with no counterpart here
 * maps to {@code null} on the wire (the provider translates and drops the unknown).</p>
 */
public enum EtcItemType {
    ANCIENT_CRYSTAL_ENCHANT_ARMOR,
    ANCIENT_CRYSTAL_ENCHANT_WEAPON,
    ARROW,
    BLESS_SCROLL_ENCHANT_ARMOR,
    BLESS_SCROLL_ENCHANT_WEAPON,
    BOLT,
    CASTLE_GUARD,
    CHANGE_ATTR,
    COUPON,
    CROP,
    DYE,
    ELIXIR,
    ENSOUL_STONE,
    HARVEST,
    HERB,
    LOTTO,
    LURE,
    MATERIAL,
    MATURE_CROP,
    MONEY,
    NONE,
    PET_COLLAR,
    POTION,
    RACE_TICKET,
    RECIPE,
    RUNE,
    RUNE_SELECT,
    SCROLL,
    SCROLL_ENCHANT_ARMOR,
    SCROLL_ENCHANT_ATTR,
    SCROLL_ENCHANT_WEAPON,
    SCROLL_INC_ENCHANT_PROP_ARMOR,
    SCROLL_INC_ENCHANT_PROP_WEAPON,
    SEED,
    SEED_2,
    SHOT,
    SPELLBOOK,
    TICKET_OF_LORD
}
