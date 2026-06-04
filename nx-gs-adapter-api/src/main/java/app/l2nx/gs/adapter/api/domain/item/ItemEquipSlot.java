package app.l2nx.gs.adapter.api.domain.item;

/**
 * Equipment slot an item occupies — the canonical, build-agnostic equip-slot
 * vocabulary. A shared item-domain enum (not tied to one wire DTO); a provider maps
 * its core's internal slot representation (bitmask, code, or name) onto these.
 *
 * <p>{@code null} on the wire means the item is not equippable (or the build supplied
 * no slot). Dual-target slots (an item that fits either of a pair) are single
 * canonical constants: {@link #EAR}, {@link #FINGER}, {@link #CHEST_LEGS}.</p>
 */
public enum ItemEquipSlot {
    R_HAND,
    L_HAND,
    LR_HAND,
    CHEST,
    LEGS,
    CHEST_LEGS,
    FULL_ARMOR,
    HEAD,
    FEET,
    GLOVES,
    BACK,
    NECK,
    UNDERWEAR,
    HAIR,
    HAIR2,
    HAIRALL,
    DECO,
    BELT,
    L_BRACELET,
    R_BRACELET,
    BROOCH,
    BROOCH_JEWEL,
    AGATHION,
    ALLDRESS,
    EAR,
    FINGER,
    WOLF,
    GREATWOLF,
    HATCHLING,
    STRIDER,
    BABYPET
}
