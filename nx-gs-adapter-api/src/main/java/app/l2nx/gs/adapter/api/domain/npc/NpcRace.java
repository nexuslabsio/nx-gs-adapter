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
    MAGICCREATURE,
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
    FAIRIE,
    HUMAN,
    ELVE,
    DARKELVE,
    ORC,
    DWARVE,
    OTHER,
    NONLIVING,
    SIEGEWEAPON,
    DEFENDINGARMY,
    MERCENARIE,
    UNKNOWN,
    KAMAEL,
    NONE
}
