package app.l2nx.gs.adapter.api.domain.npc;

/**
 * NPC race. Most cores derive it from marker skill {@code 4416} (level encodes the race); the provider
 * resolves it. {@code null} or {@link #NONE} means no race.
 */
public enum NpcRace {
    UNDEAD,
    MAGIC_CREATURE,
    BEAST,
    ANIMAL,
    PLANT,
    HUMANOID,
    SPIRIT,
    ANGEL,
    DEMON,
    DRAGON,
    GIANT,
    BUG,
    FAIRY,
    HUMAN,
    ELF,
    DARK_ELF,
    ORC,
    DWARF,
    OTHER,
    NON_LIVING,
    SIEGE_WEAPON,
    DEFENDING_ARMY,
    MERCENARY,
    UNKNOWN_CREATURE,
    KAMAEL,
    NONE
}
