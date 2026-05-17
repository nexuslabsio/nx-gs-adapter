package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.commons.bytes.LongBytes;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

/**
 * Hardcoded type-to-wire-metadata registry for outbound events. One entry
 * per concrete event class shipped in {@code nx-gs-adapter-api}; adding a new
 * concrete event type means appending one {@code register(...)} call here.
 *
 * <p>Not pluggable — once 3+ event families exist, this graduates to a
 * proper SPI. YAGNI for now.</p>
 *
 * <p>Package-private. External callers go through {@link EventsBootstrap}
 * which owns construction; the registry's {@link #lookup} and
 * {@link #knownFamilies} accessors are consumed only by classes in this
 * package.</p>
 */
final class EventTypeRegistry {

    private final Map<Class<?>, EventTypeBinding> bindings;
    private final Set<String> familyKeys;

    EventTypeRegistry() {
        Map<Class<?>, EventTypeBinding> map = new HashMap<>();
        Set<String> families = new LinkedHashSet<>();

        register(map, families, PremiumPurchaseEvent.class, "premiumpurchase",
                evt -> LongBytes.bigEndian(((PremiumPurchaseEvent) evt).getCharacterId()));

        // Snapshots have no natural per-entity partition key; null → round-robin,
        // consumers group/order by Nx-Server-Id header + UUIDv7 eventId.
        register(map, families, ServerOnlineSnapshotEvent.class, "serveronline",
                evt -> null);

        // Purchase: two parties, no single natural key → round-robin.
        register(map, families, PrivateStorePurchaseEvent.class, "privatestore",
                evt -> null);
        // Snapshot: partition by itemId — order book per item lands on one partition.
        register(map, families, PrivateStoreSnapshotEvent.class, "privatestore",
                evt -> LongBytes.bigEndian(((PrivateStoreSnapshotEvent) evt).getItemId()));

        // Character presence: partition by charId — per-character history ordered.
        register(map, families, CharacterPresenceEvent.class, "character",
                evt -> LongBytes.bigEndian(((CharacterPresenceEvent) evt).getCharId()));

        this.bindings = Collections.unmodifiableMap(map);
        this.familyKeys = Collections.unmodifiableSet(families);
    }

    /**
     * Lookup a binding by concrete class. Returns {@code null} when the
     * class is not registered — caller logs and drops.
     */
    @Nullable
    EventTypeBinding lookup(Class<?> type) {
        return bindings.get(type);
    }

    /**
     * All known family keys, in declaration order. Used by
     * {@link EventsPublisher} to compute the {@code disabled-families}
     * heartbeat slot.
     */
    Set<String> knownFamilies() {
        return familyKeys;
    }

    private static void register(Map<Class<?>, EventTypeBinding> map, Set<String> families,
                                 Class<?> type, String familyKey,
                                 Function<Object, byte[]> partitionKeyExtractor) {
        map.put(type, new EventTypeBinding(familyKey, type.getSimpleName(), partitionKeyExtractor));
        families.add(familyKey);
    }
}
