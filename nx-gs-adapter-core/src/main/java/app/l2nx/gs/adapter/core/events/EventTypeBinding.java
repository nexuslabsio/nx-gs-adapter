package app.l2nx.gs.adapter.core.events;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Hardcoded binding from a concrete event class to its wire metadata —
 * the family key for topic lookup, the {@code Nx-Message-Type} header value,
 * and a partition-key extractor.
 *
 * <p>{@link #messageTypeBytes()} pre-encodes the header value once at registry
 * construction so the daemon thread doesn't re-encode the same UTF-8 bytes
 * per envelope.</p>
 */
final class EventTypeBinding {

    private final String familyKey;
    private final String messageType;
    private final byte[] messageTypeBytes;
    private final Function<Object, byte[]> partitionKeyExtractor;

    EventTypeBinding(String familyKey, String messageType, Function<Object, byte[]> partitionKeyExtractor) {
        this.familyKey = familyKey;
        this.messageType = messageType;
        this.messageTypeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    String familyKey() {
        return familyKey;
    }

    String messageType() {
        return messageType;
    }

    byte[] messageTypeBytes() {
        return messageTypeBytes;
    }

    Function<Object, byte[]> partitionKeyExtractor() {
        return partitionKeyExtractor;
    }
}
