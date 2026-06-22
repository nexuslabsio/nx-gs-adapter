package app.l2nx.gs.commons.bytes;

/**
 * Big-endian {@code long} ↔ {@code byte[8]} codec used by adapter modules to
 * encode primitive partition keys (CDC pkAsLong, char id partition keys for
 * events / commands) onto Kafka records as 8 raw bytes.
 *
 * <p>Pure JDK, zero dep, hot-path-friendly — no {@link java.nio.ByteBuffer}
 * allocation per call (the wrapper object would double the per-record garbage
 * for what is conceptually one {@code byte[8]} write).</p>
 */
public final class LongBytes {

    private LongBytes() {}

    /**
     * Encode {@code value} as 8 big-endian bytes. Suitable for direct use as a
     * Kafka record key when the key is conceptually a {@code long} (FNV-1a-64
     * hash, character id, correlation-id-most-bits, …).
     *
     * @param value 64-bit value to encode
     * @return new {@code byte[8]} with the most-significant byte first
     */
    public static byte[] bigEndian(long value) {
        byte[] out = new byte[8];
        out[0] = (byte) (value >>> 56);
        out[1] = (byte) (value >>> 48);
        out[2] = (byte) (value >>> 40);
        out[3] = (byte) (value >>> 32);
        out[4] = (byte) (value >>> 24);
        out[5] = (byte) (value >>> 16);
        out[6] = (byte) (value >>> 8);
        out[7] = (byte) value;
        return out;
    }
}
