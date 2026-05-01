package app.l2nx.gs.adapter.api.domain.character;

/**
 * Player character race.
 *
 * <p>Mirrors the L2 race set as it stabilized in High Five / Salvation
 * (Kamael added in C5; Ertheia added in Goddess of Destruction is not part
 * of this enum, as Salvation-era forks do not surface it). Schema providers
 * translate source-side numeric race codes (canonical ordinals:
 * {@code 0=HUMAN, 1=ELF, 2=DARK_ELF, 3=ORC, 4=DWARF, 5=KAMAEL}) in their
 * {@code PrimarySource.mapRow()} implementation. Unknown source codes are
 * logged as warnings and surface as {@code null}.</p>
 */
public enum CharacterRace {
    HUMAN,
    ELF,
    DARK_ELF,
    ORC,
    DWARF,
    KAMAEL
}
