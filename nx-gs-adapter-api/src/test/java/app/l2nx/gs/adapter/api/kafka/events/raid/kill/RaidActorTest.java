package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RaidActorTest {

    @Test
    void nullableFields_shouldBeNullByDefault() {
        RaidActor a = RaidActor.builder().charId(42L).build();

        assertNull(a.getClanId());
        assertNull(a.getAllyId());
        assertNull(a.getPartyId());
        assertNull(a.getCommandChannelId());
        assertEquals(0L, a.getDamageDealt());
    }

    @Test
    void builder_shouldPopulateAllFields() {
        UUID partyId = UUID.randomUUID();
        UUID ccId = UUID.randomUUID();
        RaidActor a = RaidActor.builder()
                .charId(268415943L)
                .clanId(268440117L)
                .allyId(268455901L)
                .partyId(partyId)
                .commandChannelId(ccId)
                .damageDealt(145820L)
                .build();

        assertEquals(268415943L, a.getCharId());
        assertEquals(Long.valueOf(268440117L), a.getClanId());
        assertEquals(Long.valueOf(268455901L), a.getAllyId());
        assertEquals(partyId, a.getPartyId());
        assertEquals(ccId, a.getCommandChannelId());
        assertEquals(145820L, a.getDamageDealt());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        RaidActor original = RaidActor.builder()
                .charId(268415943L)
                .clanId(268440117L)
                .partyId(UUID.randomUUID())
                .damageDealt(145820L)
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishCharId() {
        RaidActor a = RaidActor.builder().charId(1L).damageDealt(100L).build();
        RaidActor b = RaidActor.builder().charId(2L).damageDealt(100L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishDamageDealt() {
        RaidActor a = RaidActor.builder().charId(1L).damageDealt(100L).build();
        RaidActor b = RaidActor.builder().charId(1L).damageDealt(200L).build();

        assertNotEquals(a, b);
    }
}
