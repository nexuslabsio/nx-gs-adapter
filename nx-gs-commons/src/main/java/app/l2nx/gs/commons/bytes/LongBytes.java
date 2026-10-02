package app.l2nx.gs.commons.bytes;

/** Big-endian long to byte[8] without ByteBuffer allocation, for hot-path Kafka keys. */
public final class LongBytes {

    private LongBytes() {}

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
