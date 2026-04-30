package app.l2nx.gs.adapter.api.domain;

/**
 * Player character sex.
 *
 * <p>L2 player characters have only two sex values; the L2J-family runtime
 * carries a third {@code ETC} state for NPC-only mobs, which is excluded
 * from this enum since it is not valid for the {@code characters} table.
 * Schema providers translate the source-side ordinal in
 * {@code PrimarySource.mapRow()}: {@code 0=MALE, 1=FEMALE}. Unknown source
 * codes (e.g. NPC {@code ETC} appearing in characters by mistake) are
 * logged as warnings and surface as {@code null}.</p>
 */
public enum Sex {
    MALE,
    FEMALE
}
