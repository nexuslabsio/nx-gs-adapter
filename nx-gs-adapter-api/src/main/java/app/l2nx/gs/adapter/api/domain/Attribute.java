package app.l2nx.gs.adapter.api.domain;

/**
 * Elemental attribute shared by item, skill and npc. Absence (source sentinel {@code -1}, a
 * {@code NONE} marker, unknown codes) is {@code null} on the wire, never a constant.
 */
public enum Attribute {
    FIRE,
    WATER,
    WIND,
    EARTH,
    HOLY,
    DARK
}
