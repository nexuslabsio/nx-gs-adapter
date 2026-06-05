package app.l2nx.gs.adapter.api.domain.npc;

/**
 * Soul-absorb mode of an NPC — who, among the attackers, may absorb its soul
 * crystal. The canonical, build-agnostic absorb-type vocabulary; a provider maps
 * its core's internal absorb classification onto these.
 */
public enum NpcAbsorbType {
    LAST_HIT,
    PARTY_ONE,
    PARTY_ALL,
    PARTY_RANDOM
}
