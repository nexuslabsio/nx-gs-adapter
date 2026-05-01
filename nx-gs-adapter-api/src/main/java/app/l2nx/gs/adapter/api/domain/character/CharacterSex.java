package app.l2nx.gs.adapter.api.domain.character;

/**
 * Player character sex.
 *
 * <p>L2 player characters have only two sex values; source-side runtimes
 * carry a third {@code ETC} state for NPC-only mobs, which is excluded from
 * this enum since it is not valid for the {@code characters} table. Schema
 * providers translate the source-side ordinal in
 * {@code PrimarySource.mapRow()}: {@code 0=MALE, 1=FEMALE}. Unknown source
 * codes (e.g. NPC {@code ETC} appearing in characters by mistake) are
 * logged as warnings and surface as {@code null}.</p>
 */
public enum CharacterSex {
    MALE,
    FEMALE
}
