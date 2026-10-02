package app.l2nx.gs.adapter.api.domain.item;

/**
 * Item storage location. Source locations not listed (VOID, LEASE, REFUND, FREIGHT, AUCTION,
 * alt-storage variants) and unknown ones surface as {@code null}.
 */
public enum ItemLocation {
    INVENTORY,
    EQUIP,
    WH,
    CLAN_WH,
    PET_INVENTORY,
    PET_EQUIP,
    MAIL
}
