package app.l2nx.gs.adapter.api.kafka;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Wire-level Kafka header constants and value codecs shared between the
 * {@code nx-gs-adapter} producer side and platform consumers.
 */
public final class NxHeaders {

    /**
     * Header carrying the originating game-server id. Value is encoded via
     * {@link #encodeUuid(UUID)} / {@link #decodeUuid(byte[])}.
     */
    public static final String NX_SERVER_ID = "Nx-Server-Id";

    private NxHeaders() {
    }

    /**
     * Encodes a {@link UUID} as 16 raw bytes: {@code mostSigBits} big-endian
     * followed by {@code leastSigBits} big-endian.
     */
    public static byte[] encodeUuid(UUID uuid) {
        Objects.requireNonNull(uuid, "uuid");
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    /**
     * Inverse of {@link #encodeUuid(UUID)}. Throws
     * {@link IllegalArgumentException} when input is {@code null} or not 16 bytes.
     */
    public static UUID decodeUuid(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("UUID header value must not be null");
        }
        if (value.length != 16) {
            throw new IllegalArgumentException(
                    "UUID header value must be 16 bytes, got " + value.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(value);
        long msb = buf.getLong();
        long lsb = buf.getLong();
        return new UUID(msb, lsb);
    }
}
