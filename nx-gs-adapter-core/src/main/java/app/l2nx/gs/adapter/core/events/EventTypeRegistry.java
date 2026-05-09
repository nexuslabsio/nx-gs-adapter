package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.online.OnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreTradeEvent;
import app.l2nx.gs.commons.bytes.LongBytes;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Hardcoded type-to-wire-metadata registry for outbound events. One entry
 * per concrete event class shipped in {@code nx-gs-adapter-api}; adding a new
 * concrete event type means appending one entry here.
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
        Map<Class<?>, EventTypeBinding> map = new HashMap<Class<?>, EventTypeBinding>();
        Set<String> families = new LinkedHashSet<>();

        map.put(PremiumPurchaseEvent.class, new EventTypeBinding(
                "premium",
                "PremiumPurchaseEvent",
                evt -> LongBytes.bigEndian(((PremiumPurchaseEvent) evt).getCharacterId())));
        families.add("premium");

        // Snapshots have no natural per-entity partition key; null → round-robin,
        // consumers group/order by Nx-Server-Id header + UUIDv7 eventId.
        map.put(OnlineSnapshotEvent.class, new EventTypeBinding(
                "online",
                "OnlineSnapshotEvent",
                evt -> null));
        families.add("online");

        // Trade events: two parties (buyer + seller), no single natural per-entity
        // key; null → round-robin. Per-character history is a consumer-side query
        // (filter by buyerId or sellerId, sort by UUIDv7 timestamp), not a
        // partitioning concern.
        map.put(PrivateStoreTradeEvent.class, new EventTypeBinding(
                "private_store",
                "PrivateStoreTradeEvent",
                evt -> null));
        // Snapshot events partition by itemId — all updates for the same item
        // land on the same partition for ordered consumption / topic-compaction-
        // friendly "latest known book per item" caching.
        map.put(PrivateStoreSnapshotEvent.class, new EventTypeBinding(
                "private_store",
                "PrivateStoreSnapshotEvent",
                evt -> LongBytes.bigEndian(((PrivateStoreSnapshotEvent) evt).getItemId())));
        families.add("private_store");

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
}
