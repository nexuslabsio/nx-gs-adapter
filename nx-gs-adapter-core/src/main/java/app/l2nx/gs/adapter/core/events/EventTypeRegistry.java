package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.premium.PremiumPurchaseEvent;
import app.l2nx.gs.commons.bytes.LongBytes;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Hardcoded type-to-wire-metadata registry for outbound events. One entry
 * per concrete event class shipped in {@code nx-gs-adapter-api}; adding a new
 * concrete event type means appending one entry here.
 *
 * <p>Phase 1 ships exactly one binding: {@code PremiumPurchaseEvent} →
 * family {@code "premium"} / message-type {@code "PremiumPurchaseEvent"} /
 * partition-key {@code characterId} (8 raw bytes, big-endian).</p>
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
        Set<String> families = new LinkedHashSet<String>();

        // Premium family — Phase 1 single concrete subtype.
        map.put(PremiumPurchaseEvent.class, new EventTypeBinding(
                "premium",
                "PremiumPurchaseEvent",
                evt -> LongBytes.bigEndian(((PremiumPurchaseEvent) evt).getCharacterId())));
        families.add("premium");

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
