package app.l2nx.gs.adapter.api.domain;

/**
 * Elemental attribute kind — the canonical, build-agnostic L2 element vocabulary shared
 * across item (elemental attack/defence), skill (skill element) and npc. A cross-entity
 * domain enum (not tied to one slice or wire DTO); a provider maps its core's internal
 * element representation onto these constants.
 *
 * <p>Schema providers translate source-side numeric {@code elemType} codes (canonical
 * convention: {@code 0=FIRE}, {@code 1=WATER}, {@code 2=WIND}, {@code 3=EARTH},
 * {@code 4=HOLY}, {@code 5=DARK}) into this enum. The source-side runtime sentinel
 * {@code -1} ("no attribute") / a {@code NONE} marker has no wire representation —
 * absence surfaces as {@code null}. Unknown source codes are logged by the provider and
 * surface as {@code null} on the wire.</p>
 */
public enum Attribute {
    FIRE,
    WATER,
    WIND,
    EARTH,
    HOLY,
    DARK
}
