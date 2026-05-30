package app.l2nx.gs.adapter.api.kafka.events.raid.respawn;

import app.l2nx.gs.adapter.api.kafka.events.raid.RaidBossKind;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BossRespawnSnapshotEventTest {

    private static UUID id() {
        return UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
    }

    private static BossRespawnEntry deadBoss() {
        return BossRespawnEntry.builder()
                .npcId(29020)
                .level(75)
                .kind(RaidBossKind.GRAND_BOSS)
                .status(WellKnownBossStatuses.DEAD)
                .nextRespawnAt(Instant.parse("2026-06-01T12:00:00Z"))
                .build();
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(NullPointerException.class, () -> BossRespawnSnapshotEvent.builder().build());
    }

    @Test
    void getBosses_shouldReturnEmptyList_whenBuilderOmits() {
        BossRespawnSnapshotEvent event = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .build();

        assertTrue(event.getBosses().isEmpty());
        assertNull(event.getMetadata());
    }

    @Test
    void getBosses_shouldBeUnmodifiable() {
        BossRespawnSnapshotEvent event = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .bosses(new ArrayList<>(Collections.singletonList(deadBoss())))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getBosses().add(deadBoss()));
    }

    @Test
    void constructor_shouldDefensivelyCopyBossesList() {
        List<BossRespawnEntry> source = new ArrayList<>();
        source.add(deadBoss());

        BossRespawnSnapshotEvent event = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .bosses(source)
                .build();

        source.add(deadBoss());

        assertEquals(1, event.getBosses().size());
    }

    @Test
    void getMetadata_shouldBeUnmodifiable_whenPresent() {
        BossRespawnSnapshotEvent event = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .metadata(Collections.singletonMap("source", "raidbossmanager"))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getMetadata().put("k", "v"));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        BossRespawnSnapshotEvent original = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .bosses(Collections.singletonList(deadBoss()))
                .metadata(Collections.singletonMap("source", "raidbossmanager"))
                .build();

        BossRespawnSnapshotEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishBosses() {
        BossRespawnSnapshotEvent empty = BossRespawnSnapshotEvent.builder().eventId(id()).build();
        BossRespawnSnapshotEvent withBoss = BossRespawnSnapshotEvent.builder()
                .eventId(id())
                .bosses(Collections.singletonList(deadBoss()))
                .build();

        assertNotEquals(empty, withBoss);
    }

    @Test
    void entry_aliveBoss_shouldCarryNullRespawn() {
        BossRespawnEntry alive = BossRespawnEntry.builder()
                .npcId(25035)
                .level(40)
                .kind(RaidBossKind.RAID)
                .status(WellKnownBossStatuses.ALIVE)
                .build();

        assertEquals(WellKnownBossStatuses.ALIVE, alive.getStatus());
        assertNull(alive.getNextRespawnAt());
        assertNull(alive.getMetadata());
        assertEquals(RaidBossKind.RAID, alive.getKind());
    }

    @Test
    void entry_metadata_shouldBeUnmodifiableAndDefensivelyCopied() {
        Map<String, String> source = new HashMap<>();
        source.put("zone", "antharas-lair");

        BossRespawnEntry entry = BossRespawnEntry.builder()
                .npcId(29019)
                .kind(RaidBossKind.GRAND_BOSS)
                .status(WellKnownBossStatuses.DEAD)
                .nextRespawnAt(Instant.parse("2026-06-02T00:00:00Z"))
                .metadata(source)
                .build();

        source.put("late", "value");

        assertEquals(1, entry.getMetadata().size());
        assertThrows(UnsupportedOperationException.class,
                () -> entry.getMetadata().put("k", "v"));
    }

    @Test
    void entry_toBuilder_shouldRoundtrip() {
        BossRespawnEntry original = deadBoss();
        BossRespawnEntry copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertNotSame(original, copy);
    }
}
