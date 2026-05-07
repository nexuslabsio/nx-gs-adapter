package app.l2nx.gs.commons;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * UUIDv7 generator per <a href="https://datatracker.ietf.org/doc/rfc9562/">RFC 9562</a>:
 * a time-ordered UUID carrying a millisecond-precision Unix timestamp in the upper
 * 48 bits, followed by a 4-bit version, 12 bits of monotonic counter, a 2-bit
 * variant marker, and 62 bits of randomness.
 *
 * <p>Why time-ordered: UUIDs sort lexicographically by creation time, which lets
 * platform consumers extract {@code occurredAt} from the id alone — no separate
 * timestamp field on every event payload.</p>
 *
 * <p>Pure JDK; no third-party dependency. Diverges from
 * {@code app.l2nx.common.UUIDv7} in the {@code nx-libs} platform repo (which
 * uses {@code com.fasterxml.uuid:java-uuid-generator} and Java-11
 * {@code String.isBlank}) — this module ships under the open-core
 * Java-8 + JSpecify-only deps charter.</p>
 *
 * <p>Monotonicity: a class-level mutex tracks the last emitted timestamp and a
 * 12-bit sub-millisecond counter. Calls landing in the same millisecond
 * increment the counter (4096 ids/ms before exhaustion). On counter exhaustion
 * the logical timestamp advances by 1 ms — strictly increasing ids never
 * regress, even under burst or system-clock backsteps.</p>
 *
 * <pre>
 * UUID id = UUIDv7.generate();
 * Instant when = UUIDv7.extractCreatedAt(id);
 * </pre>
 */
public final class UUIDv7 {

    private static final long TS_MASK = 0x0000_FFFF_FFFF_FFFFL; // low 48 bits
    private static final long VERSION_BITS = 0x7L << 12;        // version = 7 in MSB bits 12..15
    private static final int COUNTER_MAX = 0x0FFF;             // 12 bits
    private static final long VARIANT_CLEAR_MASK = 0x3FFF_FFFF_FFFF_FFFFL; // clears top 2 LSB bits
    private static final long VARIANT_SET_BIT = 0x8000_0000_0000_0000L;    // sets top LSB bit -> variant 10

    private static final Object LOCK = new Object();
    private static long lastTimestampMs = -1L;
    private static int subMsCounter = 0;

    private UUIDv7() {
    }

    /**
     * Generate a fresh UUIDv7. Strictly monotonic within a JVM — repeated calls
     * always return ids that compare as greater than every previously-returned id.
     */
    public static UUID generate() {
        long timestampMs;
        int counter;

        synchronized (LOCK) {
            long nowMs = System.currentTimeMillis();
            if (nowMs > lastTimestampMs) {
                // Wall clock advanced — start a fresh sub-ms counter at 0.
                lastTimestampMs = nowMs;
                subMsCounter = 0;
            }
            // Same-or-backwards clock: stay on the last emitted ms and step the
            // counter. On counter exhaustion, advance the logical ms by 1 —
            // preserves monotonicity at the cost of a slight forward skew until
            // wall-clock catches up.
            if (subMsCounter > COUNTER_MAX) {
                lastTimestampMs += 1;
                subMsCounter = 0;
            }
            timestampMs = lastTimestampMs;
            counter = subMsCounter++;
        }

        long msb = (timestampMs & TS_MASK) << 16;
        msb |= VERSION_BITS;
        msb |= (counter & 0x0FFFL);

        long lsb = ThreadLocalRandom.current().nextLong();
        lsb &= VARIANT_CLEAR_MASK;
        lsb |= VARIANT_SET_BIT;

        return new UUID(msb, lsb);
    }

    /**
     * Extract the embedded Unix-epoch-ms timestamp from a UUIDv7. Returns
     * {@code null} for {@code null} input — convenient for consumers reading an
     * optional {@code @Nullable UUID} field. Throws {@link IllegalArgumentException}
     * for non-version-7 ids.
     */
    public static @Nullable Instant extractCreatedAt(@Nullable UUID uuid) {
        if (uuid == null) {
            return null;
        }
        if (uuid.version() != 7) {
            throw new IllegalArgumentException("Expected UUIDv7 but got version " + uuid.version());
        }
        long ms = uuid.getMostSignificantBits() >>> 16;
        return Instant.ofEpochMilli(ms);
    }

    /**
     * Tolerant variant of {@link UUID#fromString(String)}: returns {@code null}
     * for {@code null} or whitespace-only input. Use for parsing optional headers
     * ({@code Nx-Server-Id}, {@code Nx-Correlation-Id}, etc.) where absence means
     * "no value", not "error". For required ids call {@link UUID#fromString(String)}
     * directly so a missing value surfaces as a clean exception.
     */
    public static @Nullable UUID fromString(@Nullable String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return UUID.fromString(trimmed);
    }
}
