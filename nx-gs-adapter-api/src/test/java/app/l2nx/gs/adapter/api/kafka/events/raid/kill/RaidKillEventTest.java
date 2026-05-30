package app.l2nx.gs.adapter.api.kafka.events.raid.kill;

import app.l2nx.gs.adapter.api.kafka.events.raid.RaidBossKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RaidKillEventTest {

    @Test
    void constructor_shouldThrow_whenEventIdNull() {
        assertThrows(NullPointerException.class, () -> RaidKillEvent.builder()
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .build());
    }

    @Test
    void constructor_shouldThrow_whenBossKindNull() {
        assertThrows(NullPointerException.class, () -> RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .build());
    }

    @Test
    void getParticipants_shouldReturnEmptyList_whenBuilderOmits() {
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .build();

        assertTrue(event.getParticipants().isEmpty());
    }

    @Test
    void getDrops_shouldReturnEmptyList_whenBuilderOmits() {
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .build();

        assertTrue(event.getDrops().isEmpty());
    }

    @Test
    void nullableFields_shouldBeNullByDefault() {
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .build();

        assertNull(event.getBossName());
        assertNull(event.getBossLevel());
        assertNull(event.getInstanceId());
        assertNull(event.getLastHit());
        assertNull(event.getDropOwner());
    }

    @Test
    void getParticipants_shouldBeUnmodifiable() {
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .participants(Collections.singletonList(
                        RaidActor.builder().charId(1L).damageDealt(100L).build()))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getParticipants().add(null));
    }

    @Test
    void getDrops_shouldBeUnmodifiable() {
        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .drops(Collections.singletonList(
                        RaidDropItem.builder().itemId(57).count(1L).build()))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getDrops().add(null));
    }

    @Test
    void constructor_shouldDefensivelyCopyParticipantsList() {
        List<RaidActor> source = new ArrayList<>();
        source.add(RaidActor.builder().charId(1L).damageDealt(100L).build());

        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .participants(source)
                .build();

        source.add(RaidActor.builder().charId(2L).damageDealt(200L).build());

        assertEquals(1, event.getParticipants().size());
    }

    @Test
    void constructor_shouldDefensivelyCopyDropsList() {
        List<RaidDropItem> source = new ArrayList<>();
        source.add(RaidDropItem.builder().itemId(57).count(1L).build());

        RaidKillEvent event = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .drops(source)
                .build();

        source.add(RaidDropItem.builder().itemId(6660).count(1L).build());

        assertEquals(1, event.getDrops().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        UUID partyId = UUID.randomUUID();
        UUID ccId = UUID.randomUUID();
        RaidActor killer = RaidActor.builder()
                .charId(268415943L)
                .clanId(268440117L)
                .partyId(partyId)
                .commandChannelId(ccId)
                .damageDealt(145820L)
                .build();
        RaidKillEvent original = RaidKillEvent.builder()
                .eventId(UUID.randomUUID())
                .bossNpcId(29028)
                .bossName("Valakas")
                .bossLevel(85)
                .bossKind(RaidBossKind.GRAND_BOSS)
                .lastHit(killer)
                .dropOwner(killer)
                .participants(Collections.singletonList(killer))
                .drops(Collections.singletonList(
                        RaidDropItem.builder().itemId(6660).count(1L).build()))
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
