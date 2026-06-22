package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ServerOnlineSnapshotEventTest {

    @Test
    void getBuckets_shouldReturnEmptyMap_whenBuilderOmits() {
        ServerOnlineSnapshotEvent event =
                ServerOnlineSnapshotEvent.builder().eventId(UUID.randomUUID()).build();

        assertTrue(event.getBuckets().isEmpty());
    }

    @Test
    void getBuckets_shouldReturnEmptyMap_whenBuilderPassesNull() {
        ServerOnlineSnapshotEvent event = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(null)
                .build();

        assertTrue(event.getBuckets().isEmpty());
    }

    @Test
    void getBuckets_shouldBeUnmodifiable() {
        Map<String, Long> source = new HashMap<String, Long>();
        source.put(WellKnownServerOnlineBuckets.TOTAL, 100L);

        ServerOnlineSnapshotEvent event = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(source)
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> event.getBuckets().put(WellKnownServerOnlineBuckets.UNIQUE, 99L));
    }

    @Test
    void constructor_shouldDefensivelyCopyBucketsMap() {
        Map<String, Long> source = new HashMap<String, Long>();
        source.put(WellKnownServerOnlineBuckets.TOTAL, 100L);

        ServerOnlineSnapshotEvent event = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(source)
                .build();

        source.put(WellKnownServerOnlineBuckets.UNIQUE, 95L);

        assertEquals(1, event.getBuckets().size());
        assertEquals(Long.valueOf(100L), event.getBuckets().get(WellKnownServerOnlineBuckets.TOTAL));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<String, Long> buckets = new LinkedHashMap<String, Long>();
        buckets.put(WellKnownServerOnlineBuckets.TOTAL, 1808L);
        buckets.put(WellKnownServerOnlineBuckets.UNIQUE, 1640L);
        buckets.put(WellKnownServerOnlineBuckets.OFFLINE_TRADE, 22L);
        buckets.put(WellKnownServerOnlineBuckets.FISHING, 15L);

        ServerOnlineSnapshotEvent original = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();

        ServerOnlineSnapshotEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishEventId() {
        Map<String, Long> buckets = new HashMap<String, Long>();
        buckets.put(WellKnownServerOnlineBuckets.TOTAL, 100L);

        ServerOnlineSnapshotEvent a = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();
        ServerOnlineSnapshotEvent b = ServerOnlineSnapshotEvent.builder()
                .eventId(UUID.randomUUID())
                .buckets(buckets)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishBucketCounts() {
        UUID id = UUID.randomUUID();
        Map<String, Long> bucketsA = new HashMap<String, Long>();
        bucketsA.put(WellKnownServerOnlineBuckets.TOTAL, 100L);
        Map<String, Long> bucketsB = new HashMap<String, Long>();
        bucketsB.put(WellKnownServerOnlineBuckets.TOTAL, 101L);

        ServerOnlineSnapshotEvent a = ServerOnlineSnapshotEvent.builder()
                .eventId(id)
                .buckets(bucketsA)
                .build();
        ServerOnlineSnapshotEvent b = ServerOnlineSnapshotEvent.builder()
                .eventId(id)
                .buckets(bucketsB)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void toString_shouldRenderEventIdAndBuckets() {
        UUID id = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");
        Map<String, Long> buckets = new LinkedHashMap<String, Long>();
        buckets.put(WellKnownServerOnlineBuckets.TOTAL, 1808L);

        ServerOnlineSnapshotEvent event =
                ServerOnlineSnapshotEvent.builder().eventId(id).buckets(buckets).build();

        String s = event.toString();
        assertTrue(s.contains("eventId=" + id));
        assertTrue(s.contains("total"));
        assertTrue(s.contains("1808"));
    }
}
