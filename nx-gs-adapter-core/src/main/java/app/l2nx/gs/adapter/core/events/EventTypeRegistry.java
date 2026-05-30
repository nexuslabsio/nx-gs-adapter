package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.account.AccountAuthAttemptEvent;
import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.gameevents.GameEventSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailAcceptedEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailCancelledEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailReturnedEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.MailSentEvent;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.HeroGrantedEvent;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadMatchResultEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatetrade.PrivateTradeFinishedEvent;
import app.l2nx.gs.adapter.api.kafka.events.raid.kill.RaidKillEvent;
import app.l2nx.gs.adapter.api.kafka.events.raid.respawn.BossRespawnSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.commons.bytes.LongBytes;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
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

        // Partition key extractor returns null = round-robin (no natural per-entity key).
        register(map, families, PremiumPurchaseEvent.class, "premiumpurchase",
                evt -> LongBytes.bigEndian(((PremiumPurchaseEvent) evt).getCharacterId()));
        register(map, families, ServerOnlineSnapshotEvent.class, "serveronline",
                evt -> null);
        register(map, families, GameEventSnapshotEvent.class, "gameevents",
                evt -> null);
        register(map, families, PrivateStorePurchaseEvent.class, "privatestore",
                evt -> null);
        register(map, families, PrivateStoreSnapshotEvent.class, "privatestore",
                evt -> LongBytes.bigEndian(((PrivateStoreSnapshotEvent) evt).getItemId()));
        register(map, families, CharacterPresenceEvent.class, "character",
                evt -> LongBytes.bigEndian(((CharacterPresenceEvent) evt).getCharId()));
        register(map, families, RaidKillEvent.class, "raid",
                evt -> LongBytes.bigEndian(((RaidKillEvent) evt).getBossNpcId()));
        // Periodic boss-respawn snapshot shares the raid family/topic (related boss
        // lifecycle); round-robin partition key like other snapshots.
        register(map, families, BossRespawnSnapshotEvent.class, "raid",
                evt -> null);
        register(map, families, MailSentEvent.class, "mail",
                evt -> LongBytes.bigEndian(((MailSentEvent) evt).getMailId()));
        register(map, families, MailAcceptedEvent.class, "mail",
                evt -> LongBytes.bigEndian(((MailAcceptedEvent) evt).getMailId()));
        register(map, families, MailCancelledEvent.class, "mail",
                evt -> LongBytes.bigEndian(((MailCancelledEvent) evt).getMailId()));
        register(map, families, MailReturnedEvent.class, "mail",
                evt -> LongBytes.bigEndian(((MailReturnedEvent) evt).getMailId()));
        register(map, families, PrivateTradeFinishedEvent.class, "privatetrade",
                evt -> null);
        register(map, families, OlympiadMatchResultEvent.class, "olympiad",
                evt -> LongBytes.bigEndian(((OlympiadMatchResultEvent) evt).getCharId()));
        register(map, families, HeroGrantedEvent.class, "olympiad",
                evt -> LongBytes.bigEndian(((HeroGrantedEvent) evt).getCharId()));
        // Login-server account auth: partition key is the lowercased account name
        // so per-account attempt history lands in one partition in occurrence order.
        register(map, families, AccountAuthAttemptEvent.class, "account",
                evt -> ((AccountAuthAttemptEvent) evt).getAccountName()
                        .toLowerCase(Locale.ROOT)
                        .getBytes(StandardCharsets.UTF_8));

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
