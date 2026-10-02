package app.l2nx.gs.adapter.core.events;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/** {@link #messageTypeBytes()} is pre-encoded once so the daemon thread does not re-encode per envelope. */
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
