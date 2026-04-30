package app.l2nx.gs.adapter.api.domain;

/**
 * Storage location for one item — minimal set surfaced on the wire.
 *
 * <p>Schema providers translate the source string into this enum. Source
 * locations not represented here (e.g. L2J's {@code VOID} transient state,
 * {@code LEASE} / {@code REFUND} / {@code FREIGHT} / {@code AUCTION}
 * intermediate states, alt-storage variants) map to {@code null} on the
 * wire — items in such states surface as {@code location: null} in their
 * DTO. Source values truly unknown to the schema provider are logged as
 * warnings and also surface as {@code null}.</p>
 */
public enum ItemLocation {
    /**
     * Player inventory.
     */
    INVENTORY,
    /**
     * Equipped on the player.
     */
    EQUIP,
    /**
     * Player private warehouse.
     */
    WH,
    /**
     * Clan warehouse.
     */
    CLAN_WH,
    /**
     * Pet inventory.
     */
    PET_INVENTORY,
    /**
     * Equipped on a pet.
     */
    PET_EQUIP,
    /**
     * Item attached to mail.
     */
    MAIL
}
