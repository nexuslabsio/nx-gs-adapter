package app.l2nx.gs.adapter.api.kafka.events.online;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OnlineSnapshotEventTest {

    @Test
    void getBuckets_shouldReturnEmptyMap_whenBuilderOmits() {
        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .build();

        assertTrue(event.getBuckets().isEmpty());
    }

    @Test
    void getBuckets_shouldReturnEmptyMap_whenBuilderPassesNull() {
        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(null)
                .build();

        assertTrue(event.getBuckets().isEmpty());
    }

    @Test
    void getBuckets_shouldBeUnmodifiable() {
        Map<String, Long> source = new HashMap<String, Long>();
        source.put(WellKnownOnlineBuckets.TOTAL, 100L);

        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(source)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getBuckets().put(WellKnownOnlineBuckets.REAL, 99L));
    }

    @Test
    void constructor_shouldDefensivelyCopyBucketsMap() {
        Map<String, Long> source = new HashMap<String, Long>();
        source.put(WellKnownOnlineBuckets.TOTAL, 100L);

        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(source)
                .build();

        // External mutation must not leak in.
        source.put(WellKnownOnlineBuckets.REAL, 95L);

        assertEquals(1, event.getBuckets().size());
        assertEquals(Long.valueOf(100L), event.getBuckets().get(WellKnownOnlineBuckets.TOTAL));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<String, Long> buckets = new LinkedHashMap<String, Long>();
        buckets.put(WellKnownOnlineBuckets.TOTAL, 1808L);
        buckets.put(WellKnownOnlineBuckets.ONLINE, 1786L);
        buckets.put(WellKnownOnlineBuckets.REAL, 1711L);
        buckets.put(WellKnownOnlineBuckets.OFFLINE_TRADE, 22L);
        buckets.put(WellKnownOnlineBuckets.FISHING, 15L);
        buckets.put(WellKnownOnlineBuckets.PHANTOMS, 75L);

        OnlineSnapshotEvent original = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();

        OnlineSnapshotEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishEventId() {
        Map<String, Long> buckets = new HashMap<String, Long>();
        buckets.put(WellKnownOnlineBuckets.TOTAL, 100L);

        OnlineSnapshotEvent a = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();
        OnlineSnapshotEvent b = OnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishBucketCounts() {
        UUID id = UUID.randomUUID();
        Map<String, Long> bucketsA = new HashMap<String, Long>();
        bucketsA.put(WellKnownOnlineBuckets.TOTAL, 100L);
        Map<String, Long> bucketsB = new HashMap<String, Long>();
        bucketsB.put(WellKnownOnlineBuckets.TOTAL, 101L);

        OnlineSnapshotEvent a = OnlineSnapshotEvent.builder().eventId(id).buckets(bucketsA).build();
        OnlineSnapshotEvent b = OnlineSnapshotEvent.builder().eventId(id).buckets(bucketsB).build();

        assertNotEquals(a, b);
    }

    @Test
    void toString_shouldRenderEventIdAndBuckets() {
        UUID id = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        Map<String, Long> buckets = new LinkedHashMap<String, Long>();
        buckets.put(WellKnownOnlineBuckets.TOTAL, 1808L);

        OnlineSnapshotEvent event = OnlineSnapshotEvent.builder()
                .eventId(id)
                .buckets(buckets)
                .build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + id));
        assertTrue(s.contains("total"));
        assertTrue(s.contains("1808"));
    }
}
