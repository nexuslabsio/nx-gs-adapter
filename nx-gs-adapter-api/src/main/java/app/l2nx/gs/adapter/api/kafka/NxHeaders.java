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

    /**
     * Header carrying the concrete event / command type for polymorphic
     * dispatch on the platform consumer side. Value is the UTF-8-encoded
     * simple class name of the payload type (e.g. {@code "PremiumPurchaseEvent"}).
     * Adapter-core stamps this automatically on every event publish; consumers
     * switch on this header to pick the right deserializer without peeking
     * into the JSON payload.
     */
    public static final String NX_MESSAGE_TYPE = "Nx-Message-Type";

    /**
     * Header carrying the platform-issued correlation id for an inbound command
     * (Phase 2). Value is the textual UUID (string form). Reused on the
     * outbound {@code CommandResultEvent} so the platform can route the reply
     * back to the originating web-side request.
     */
    public static final String NX_CORRELATION_ID = "Nx-Correlation-Id";

    /**
     * Inbound-only header carrying the target game-server id for a command
     * record on the shared per-tenant commands topic. Encoded as 16 raw bytes
     * via {@link #encodeUuid(UUID)} / {@link #decodeUuid(byte[])}.
     *
     * <p>The platform may host multiple game-servers under a single tenant; all
     * adapters in that tenant subscribe to the same {@code <tenant>.gs.commands}
     * topic and read every record. The adapter MUST drop records whose
     * {@code Nx-Target-Server-Id} does not match its own server id (issued in
     * the {@code /connect} response). Records without the header are dropped
     * with WARN — this contract is mandatory.</p>
     *
     * <p>Distinct from {@link #NX_SERVER_ID} (outbound: "the originating
     * server"); a single record never carries both at once.</p>
     */
    public static final String NX_TARGET_SERVER_ID = "Nx-Target-Server-Id";

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
