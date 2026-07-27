package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Which class slot one of a character's classes occupies — the single {@code MAIN} class every
 * character has, versus an additional {@code SUB} class. Stable across every Salvation/High-Five-era
 * fork.
 *
 * <p>Builds disagree on where the main class lives: most keep it on the character row and only
 * subclasses in a side table, while some also store the main class in that side table under class
 * index {@code 0}. Schema providers normalize whatever their build does into this enum, so consumers
 * see one roster shape regardless of fork.</p>
 *
 * <p>Not to be confused with {@link CharacterClassType} — that is the unrelated fighter/mystic
 * division of a class.</p>
 */
public enum CharacterClassKind {
    MAIN,
    SUB
}
