package app.l2nx.gs.adapter.api.kafka.events.leveldata;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class LevelExpTableSnapshotEventTest {

    private static UUID id() {
        return UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
    }

    private static LevelExpEntry entry(int level, long requiredExp) {
        return LevelExpEntry.builder().level(level).requiredExp(requiredExp).build();
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(
                NullPointerException.class,
                () -> LevelExpTableSnapshotEvent.builder().build());
    }

    @Test
    void getLevels_shouldReturnEmptyList_whenBuilderOmits() {
        LevelExpTableSnapshotEvent event =
                LevelExpTableSnapshotEvent.builder().eventId(id()).build();

        assertTrue(event.getLevels().isEmpty());
        assertNull(event.getMetadata());
    }

    @Test
    void getLevels_shouldBeUnmodifiable() {
        LevelExpTableSnapshotEvent event = LevelExpTableSnapshotEvent.builder()
                .eventId(id())
                .levels(new ArrayList<>(Collections.singletonList(entry(1, 0L))))
                .build();

        assertThrows(
                UnsupportedOperationException.class, () -> event.getLevels().add(entry(2, 68L)));
    }

    @Test
    void constructor_shouldDefensivelyCopyLevelsList() {
        List<LevelExpEntry> source = new ArrayList<>();
        source.add(entry(1, 0L));

        LevelExpTableSnapshotEvent event = LevelExpTableSnapshotEvent.builder()
                .eventId(id())
                .levels(source)
                .build();

        source.add(entry(2, 68L));

        assertEquals(1, event.getLevels().size());
    }

    @Test
    void getMetadata_shouldBeUnmodifiable_whenPresent() {
        LevelExpTableSnapshotEvent event = LevelExpTableSnapshotEvent.builder()
                .eventId(id())
                .metadata(Collections.singletonMap("source", "experiencedata"))
                .build();

        assertThrows(
                UnsupportedOperationException.class, () -> event.getMetadata().put("k", "v"));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        LevelExpTableSnapshotEvent original = LevelExpTableSnapshotEvent.builder()
                .eventId(id())
                .levels(Arrays.asList(entry(1, 0L), entry(2, 68L)))
                .metadata(Collections.singletonMap("source", "experiencedata"))
                .build();

        LevelExpTableSnapshotEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishLevels() {
        LevelExpTableSnapshotEvent empty =
                LevelExpTableSnapshotEvent.builder().eventId(id()).build();
        LevelExpTableSnapshotEvent withLevel = LevelExpTableSnapshotEvent.builder()
                .eventId(id())
                .levels(Collections.singletonList(entry(1, 0L)))
                .build();

        assertNotEquals(empty, withLevel);
    }

    @Test
    void entry_shouldCarryLevelAndRequiredExp() {
        LevelExpEntry e = entry(80, 4_174_867_851L);

        assertEquals(80, e.getLevel());
        assertEquals(4_174_867_851L, e.getRequiredExp());
    }

    @Test
    void entry_toBuilder_shouldRoundtrip() {
        LevelExpEntry original = entry(40, 6_733_999L);
        LevelExpEntry copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertNotSame(original, copy);
    }
}
