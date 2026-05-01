package app.l2nx.gs.adapter.api.domain.item;

/**
 * Elemental attribute kind.
 *
 * <p>Mirrors the L2 elemental system. Schema providers translate source-side
 * numeric {@code elemType} codes (canonical convention: {@code 0=FIRE},
 * {@code 1=WATER}, {@code 2=WIND}, {@code 3=EARTH}, {@code 4=HOLY},
 * {@code 5=DARK}) into this enum in their {@code ChildSource.mapRow()}
 * implementation. The source-side runtime sentinel {@code -1} ("no
 * attribute") has no wire representation — it should not appear in
 * {@code item_elementals} rows in practice. Unknown source codes are
 * logged as warnings by the schema provider and surface as {@code null}
 * on the wire.</p>
 */
public enum ItemAttribute {
    FIRE,
    WATER,
    WIND,
    EARTH,
    HOLY,
    DARK
}
