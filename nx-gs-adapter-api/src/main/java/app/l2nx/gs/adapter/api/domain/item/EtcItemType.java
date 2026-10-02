package app.l2nx.gs.adapter.api.domain.item;

/**
 * Etc-item type of an {@link ItemClass#ETC} item. A source type with no counterpart here is
 * {@code null} on the wire. {@code _AM} / {@code _WP} suffixes mean armor / weapon enchant scrolls.
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
