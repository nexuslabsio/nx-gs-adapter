package app.l2nx.gs.adapter.api.domain.item;

/**
 * Equipment slot of an item; {@code null} on the wire means not equippable or no slot supplied.
 * Either-of-a-pair slots are single constants: {@link #EAR}, {@link #FINGER}, {@link #CHEST_LEGS}.
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
    HAIR_2,
    HAIR_ALL,
    DECORATION,
    BELT,
    L_BRACELET,
    R_BRACELET,
    BROOCH,
    BROOCH_JEWEL,
    AGATHION,
    ALL_DRESS,
    EAR,
    FINGER,
    WOLF,
    GREAT_WOLF,
    HATCHLING,
    STRIDER,
    BABY_PET
}
