package app.l2nx.gs.commons;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class UUIDv7Test {

    @Test
    void generate_shouldReturnVersion7Uuid() {
        UUID id = UUIDv7.generate();
        assertEquals(7, id.version());
    }

    @Test
    void generate_shouldReturnVariantTen() {
        UUID id = UUIDv7.generate();
        // Variant per RFC 9562 §4.1: top two bits of LSB are 10 — UUID#variant() returns 2.
        assertEquals(2, id.variant());
    }

    @Test
    void generate_shouldEncodeNearCurrentEpochMs() {
        long beforeMs = System.currentTimeMillis();
        UUID id = UUIDv7.generate();
        long afterMs = System.currentTimeMillis();

        Instant extracted = UUIDv7.extractCreatedAt(id);
        assertNotNull(extracted);
        long extractedMs = extracted.toEpochMilli();

        assertTrue(extractedMs >= beforeMs, "extractedMs (" + extractedMs + ") < beforeMs (" + beforeMs + ")");
        assertTrue(extractedMs <= afterMs, "extractedMs (" + extractedMs + ") > afterMs (" + afterMs + ")");
    }

    @Test
    void generate_shouldBeMonotonic_acrossBurstInSameMillisecond() {
        int n = 5000;
        UUID prev = UUIDv7.generate();
        for (int i = 1; i < n; i++) {
            UUID curr = UUIDv7.generate();
            assertTrue(curr.compareTo(prev) > 0, "non-monotonic at i=" + i + ": prev=" + prev + " curr=" + curr);
            prev = curr;
        }
    }

    @Test
    void generate_shouldProduceUniqueIds_underHighConcurrency() throws InterruptedException {
        int threads = 8;
        int perThread = 2_000;
        Set<UUID> all = java.util.Collections.synchronizedSet(new HashSet<>(threads * perThread));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        all.add(UUIDv7.generate());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "concurrent generation timed out");
        pool.shutdownNow();

        assertEquals(threads * perThread, all.size(), "duplicate ids produced under concurrency");
    }

    @Test
    void extractCreatedAt_shouldReturnNullForNull() {
        assertNull(UUIDv7.extractCreatedAt(null));
    }

    @Test
    void extractCreatedAt_shouldThrow_forNonVersion7() {
        UUID v4 = UUID.randomUUID();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> UUIDv7.extractCreatedAt(v4));
        assertTrue(ex.getMessage().contains("version"));
    }

    @Test
    void extractCreatedAt_shouldRoundTrip_withinOneMillisecond() {
        long beforeMs = System.currentTimeMillis();
        UUID id = UUIDv7.generate();
        Instant extracted = UUIDv7.extractCreatedAt(id);
        assertNotNull(extracted);
        long delta = Math.abs(extracted.toEpochMilli() - beforeMs);
        assertTrue(delta <= 1, "delta = " + delta + " ms");
    }

    @Test
    void fromString_shouldReturnNullForNull() {
        assertNull(UUIDv7.fromString(null));
    }

    @Test
    void fromString_shouldReturnNullForEmptyString() {
        assertNull(UUIDv7.fromString(""));
    }

    @Test
    void fromString_shouldReturnNullForWhitespaceOnly() {
        assertNull(UUIDv7.fromString("   \t\n  "));
    }

    @Test
    void fromString_shouldParseValidUuid() {
        UUID original = UUID.randomUUID();
        UUID parsed = UUIDv7.fromString(original.toString());
        assertNotEquals(null, parsed);
        assertEquals(original, parsed);
    }

    @Test
    void fromString_shouldTrimSurroundingWhitespace() {
        UUID original = UUID.randomUUID();
        UUID parsed = UUIDv7.fromString("  " + original + "  ");
        assertEquals(original, parsed);
    }

    @Test
    void fromString_shouldThrow_forMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> UUIDv7.fromString("not-a-uuid"));
    }
}
