package app.l2nx.gs.adapter.api.domain.npc;

/**
 * NPC race — the canonical, build-agnostic race vocabulary. A shared npc-domain
 * enum (not tied to one wire DTO); a provider maps its core's internal race
 * representation onto these.
 *
 * <p>On most L2 cores the race is derived from a marker skill (id {@code 4416},
 * whose level encodes the race) rather than a dedicated field; the provider does
 * that derivation and emits the resolved constant. {@code null} on the wire (or
 * {@link #NONE}) means the build supplied no race.</p>
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
