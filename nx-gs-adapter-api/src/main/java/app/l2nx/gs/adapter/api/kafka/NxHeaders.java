package app.l2nx.gs.adapter.api.kafka;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/** Kafka header names and UUID codecs shared by the adapter and platform consumers. */
public final class NxHeaders {

    /** Originating game-server id; value encoded via {@link #encodeUuid(UUID)}. */
    public static final String NX_SERVER_ID = "Nx-Server-Id";

    /** UTF-8 simple class name of the payload type; consumers pick the deserializer from it without parsing JSON. */
    public static final String NX_MESSAGE_TYPE = "Nx-Message-Type";

    /** Platform-issued correlation id as a textual UUID; echoed on the reply so the platform can route it back. */
    public static final String NX_CORRELATION_ID = "Nx-Correlation-Id";

    /**
     * Inbound-only target game-server id (16 raw bytes via {@link #encodeUuid(UUID)}) on the shared per-tenant commands topic.
     * The adapter MUST drop records whose id differs from its own, and records without the header (WARN).
     * Never present together with {@link #NX_SERVER_ID}.
     */
    public static final String NX_TARGET_SERVER_ID = "Nx-Target-Server-Id";

    private NxHeaders() {}

    /** Encodes as 16 bytes: most-significant long then least-significant long, big-endian. */
    public static byte[] encodeUuid(UUID uuid) {
        Objects.requireNonNull(uuid, "uuid");
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    /** Inverse of {@link #encodeUuid(UUID)}; throws {@link IllegalArgumentException} for null or non-16-byte input. */
    public static UUID decodeUuid(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("UUID header value must not be null");
        }
        if (value.length != 16) {
            throw new IllegalArgumentException("UUID header value must be 16 bytes, got " + value.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(value);
        long msb = buf.getLong();
        long lsb = buf.getLong();
        return new UUID(msb, lsb);
    }
}
