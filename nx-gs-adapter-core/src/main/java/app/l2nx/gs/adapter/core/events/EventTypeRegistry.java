package app.l2nx.gs.adapter.core.events;

import app.l2nx.gs.adapter.api.kafka.events.account.AccountAuthAttemptEvent;
import app.l2nx.gs.adapter.api.kafka.events.castle.CastleSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.castle.SiegeFinishedEvent;
import app.l2nx.gs.adapter.api.kafka.events.character.CharacterDeathEvent;
import app.l2nx.gs.adapter.api.kafka.events.character.CharacterPresenceEvent;
import app.l2nx.gs.adapter.api.kafka.events.characterlog.CharacterLogEvent;
import app.l2nx.gs.adapter.api.kafka.events.chat.ChatMessageEvent;
import app.l2nx.gs.adapter.api.kafka.events.gameevents.GameEventSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.mail.*;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.HeroGrantedEvent;
import app.l2nx.gs.adapter.api.kafka.events.olympiad.OlympiadMatchResultEvent;
import app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.PremiumPurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStoreSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.privatetrade.PrivateTradeFinishedEvent;
import app.l2nx.gs.adapter.api.kafka.events.raid.kill.RaidKillEvent;
import app.l2nx.gs.adapter.api.kafka.events.raid.respawn.BossRespawnSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerOnlineSnapshotEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerStartedEvent;
import app.l2nx.gs.adapter.api.kafka.events.serveronline.ServerStoppingEvent;
import app.l2nx.gs.adapter.api.kafka.events.sync.ResyncCompletedEvent;
import app.l2nx.gs.commons.bytes.LongBytes;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/** Not pluggable; graduate to an SPI once 3+ event families exist. */
final class EventTypeRegistry {

    private final Map<Class<?>, EventTypeBinding> bindings;
    private final Set<String> familyKeys;

    EventTypeRegistry() {
        Map<Class<?>, EventTypeBinding> map = new HashMap<>();
        Set<String> families = new LinkedHashSet<>();

        register(
                map,
                families,
                PremiumPurchaseEvent.class,
                "premiumpurchase",
                evt -> LongBytes.bigEndian(((PremiumPurchaseEvent) evt).getCharacterId()));
        register(map, families, ServerOnlineSnapshotEvent.class, "serveronline", evt -> null);
        register(map, families, ServerStartedEvent.class, "serveronline", evt -> null);
        register(map, families, ServerStoppingEvent.class, "serveronline", evt -> null);
        register(map, families, GameEventSnapshotEvent.class, "gameevents", evt -> null);
        register(map, families, PrivateStorePurchaseEvent.class, "privatestore", evt -> null);
        register(
                map,
                families,
                PrivateStoreSnapshotEvent.class,
                "privatestore",
                evt -> LongBytes.bigEndian(((PrivateStoreSnapshotEvent) evt).getItemTemplateId()));
        register(
                map,
                families,
                CharacterPresenceEvent.class,
                "character",
                evt -> LongBytes.bigEndian(((CharacterPresenceEvent) evt).getCharId()));
        register(
                map,
                families,
                CharacterDeathEvent.class,
                "character",
                evt -> LongBytes.bigEndian(((CharacterDeathEvent) evt).getCharId()));
        register(
                map,
                families,
                CharacterLogEvent.class,
                "characterlog",
                evt -> LongBytes.bigEndian(((CharacterLogEvent) evt).getCharId()));
        register(
                map,
                families,
                RaidKillEvent.class,
                "raid",
                evt -> LongBytes.bigEndian(((RaidKillEvent) evt).getBossNpcId()));
        register(map, families, BossRespawnSnapshotEvent.class, "raid", evt -> null);
        register(map, families, CastleSnapshotEvent.class, "castle", evt -> null);
        register(
                map,
                families,
                SiegeFinishedEvent.class,
                "castle",
                evt -> LongBytes.bigEndian(((SiegeFinishedEvent) evt).getCastleId()));
        register(
                map,
                families,
                MailSentEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailSentEvent) evt).getMailId()));
        register(
                map,
                families,
                MailAcceptedEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailAcceptedEvent) evt).getMailId()));
        register(
                map,
                families,
                MailCancelledEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailCancelledEvent) evt).getMailId()));
        register(
                map,
                families,
                MailReturnedEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailReturnedEvent) evt).getMailId()));
        register(
                map,
                families,
                MailReadEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailReadEvent) evt).getMailId()));
        register(
                map,
                families,
                MailDeletedEvent.class,
                "mail",
                evt -> LongBytes.bigEndian(((MailDeletedEvent) evt).getMailId()));
        register(map, families, PrivateTradeFinishedEvent.class, "privatetrade", evt -> null);
        register(
                map,
                families,
                OlympiadMatchResultEvent.class,
                "olympiad",
                evt -> LongBytes.bigEndian(((OlympiadMatchResultEvent) evt).getCharId()));
        register(
                map,
                families,
                HeroGrantedEvent.class,
                "olympiad",
                evt -> LongBytes.bigEndian(((HeroGrantedEvent) evt).getCharId()));
        register(map, families, ResyncCompletedEvent.class, "sync", evt -> null);
        register(
                map,
                families,
                ChatMessageEvent.class,
                "chat",
                evt -> LongBytes.bigEndian(((ChatMessageEvent) evt).getCharId()));
        register(
                map,
                families,
                AccountAuthAttemptEvent.class,
                "account",
                evt -> ((AccountAuthAttemptEvent) evt)
                        .getAccountName()
                        .toLowerCase(Locale.ROOT)
                        .getBytes(StandardCharsets.UTF_8));

        this.bindings = Collections.unmodifiableMap(map);
        this.familyKeys = Collections.unmodifiableSet(families);
    }

    @Nullable
    EventTypeBinding lookup(Class<?> type) {
        return bindings.get(type);
    }

    /** Declaration order. */
    Set<String> knownFamilies() {
        return familyKeys;
    }

    private static void register(
            Map<Class<?>, EventTypeBinding> map,
            Set<String> families,
            Class<?> type,
            String familyKey,
            Function<Object, byte[]> partitionKeyExtractor) {
        map.put(type, new EventTypeBinding(familyKey, type.getSimpleName(), partitionKeyExtractor));
        families.add(familyKey);
    }
}
