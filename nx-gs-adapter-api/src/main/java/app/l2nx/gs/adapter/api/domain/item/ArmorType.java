package app.l2nx.gs.adapter.api.domain.item;

/**
 * Canonical, build-agnostic armor-type vocabulary — the {@code armor_type} of an
 * {@link ItemClass#ARMOR} item. A shared item-domain enum (not tied to one wire DTO); a
 * provider maps its core's internal armor-type representation (enum, mask, or datapack
 * token) onto these.
 *
 * <p>SCREAMING_SNAKE canonical names. {@link #MAGIC} is the robe / magic-armor class;
 * {@link #SHIELD} and {@link #SIGIL} are the off-hand defensive classes; {@link #NONE} is
 * the explicit no-armor-class marker some items carry.</p>
 */
public enum ArmorType {
    HEAVY,
    LIGHT,
    MAGIC,
    NONE,
    SHIELD,
    SIGIL
}
