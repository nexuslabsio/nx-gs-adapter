package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Whether a playable class is a fighter or a mystic — the coarse L2 class division
 * ({@code isMage}). Stable across every Salvation/High-Five-era fork. Schema / game-data
 * providers translate the source-side mage flag into this enum.
 */
public enum CharacterClassType {
    FIGHTER,
    MYSTIC
}
