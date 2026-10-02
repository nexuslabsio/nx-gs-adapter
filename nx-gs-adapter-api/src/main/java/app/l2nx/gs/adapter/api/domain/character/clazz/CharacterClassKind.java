package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Class slot a character's class occupies: the single {@code MAIN} or an additional {@code SUB}.
 * Providers normalize builds that store the main class in the subclass table (index {@code 0}).
 * Unrelated to {@link CharacterClassType}.
 */
public enum CharacterClassKind {
    MAIN,
    SUB
}
