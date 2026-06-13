package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Profession tier of a playable class — its depth in the profession tree
 * ({@code BASE} → {@code FIRST} → {@code SECOND} → {@code THIRD}). Stable across every
 * Salvation/High-Five-era fork. Game-data providers translate the source-side class depth into
 * this enum.
 */
public enum CharacterClassTier {
    BASE,
    FIRST,
    SECOND,
    THIRD
}
