package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.ItemTemplateProvider;
import app.l2nx.gs.adapter.api.spi.NxGameData;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Tier-1 module that publishes static game-data (datapack-derived) templates onto
 * the {@code gd} sync stream. Reads its inputs in order:
 *
 * <ol>
 *     <li>{@code ctx.getSyncTopics().getGd()} — empty/null → DISABLED + WARN.</li>
 *     <li>Tier-2 SPI {@link ItemTemplateProvider} via {@link ServiceLoader} —
 *     0 → DISABLED + WARN, &gt;1 → FAILED, 1 → cached.</li>
 * </ol>
 *
 * <p>On {@link #start()} (when ACTIVE) the module registers a snapshot trigger on
 * {@code ctx.gameData()} — so host code that calls
 * {@code ctx.gameData().publishSnapshot()} (e.g. after an in-game datapack reload)
 * re-publishes a fresh full snapshot — and fires the initial snapshot once. Both
 * the trigger and the initial publish run on {@code ctx.io()} so they never block
 * the connect / game thread.</p>
 *
 * <p>Exception-safe throughout: every hook catches {@link Throwable} and never
 * propagates to the host JVM.</p>
 */
public final class GameDataSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSyncModule.class);

    static final String NAME = "gd-sync";

    private static final String ENTITY_ITEM_TEMPLATES = "itemtemplate";

    static final String STATE_INIT = "INIT";
    static final String STATE_DISABLED = "DISABLED";
    static final String STATE_FAILED = "FAILED";
    static final String STATE_ACTIVE = "ACTIVE";

    private final Supplier<List<ItemTemplateProvider>> providerDiscoverer;
    private final GameDataSender sender;

    private volatile String state = STATE_INIT;
    private volatile ItemTemplateProvider provider;
    private volatile ConnectContext context;
    private volatile GameDataSnapshotPublisher publisher;

    private final AtomicLong itemsPublished = new AtomicLong();
    private volatile String lastSyncId;
    private volatile long lastSyncAtEpochMs;
    private volatile int lastSnapshotItemCount;
    private volatile boolean lastSnapshotComplete;

    public GameDataSyncModule() {
        this(GameDataSyncModule::loadProviders, GameDataSyncModule::sendViaNxKafka);
    }

    GameDataSyncModule(Supplier<List<ItemTemplateProvider>> providerDiscoverer, GameDataSender sender) {
        this.providerDiscoverer = providerDiscoverer;
        this.sender = sender;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void onConnect(ConnectContext ctx) {
        this.context = ctx;
        if (ctx == null || ctx.getSyncTopics() == null
                || ctx.getSyncTopics().getGd() == null
                || ctx.getSyncTopics().getGd().isEmpty()) {
            log.warn("ConnectContext carries no gd sync topics — gd-sync DISABLED. "
                    + "Platform must publish per-entity topics under syncTopics.gd in /connect "
                    + "for the module to run.");
            state = STATE_DISABLED;
            return;
        }

        List<ItemTemplateProvider> providers = providerDiscoverer.get();
        if (providers.isEmpty()) {
            log.warn("No ItemTemplateProvider SPI registered — gd-sync DISABLED. "
                    + "Register one via META-INF/services/app.l2nx.gs.adapter.api.spi.ItemTemplateProvider "
                    + "to enable game-data sync.");
            state = STATE_DISABLED;
            return;
        }
        if (providers.size() > 1) {
            log.error("Multiple ItemTemplateProvider impls on classpath: [{}]. gd-sync FAILED.",
                    classNamesOf(providers));
            state = STATE_FAILED;
            return;
        }
        ItemTemplateProvider resolved = providers.get(0);
        log.info("ItemTemplateProvider resolved: entity={}", resolved.entityName());
        this.provider = resolved;
        this.publisher = new GameDataSnapshotPublisher(sender);
        state = STATE_ACTIVE;
    }

    @Override
    public void start() {
        if (!STATE_ACTIVE.equals(state)) {
            return;
        }
        final ConnectContext ctx = context;
        final ItemTemplateProvider p = provider;
        final GameDataSnapshotPublisher pub = publisher;
        if (ctx == null || p == null || pub == null) {
            log.error("gd-sync.start: missing dependency (context/provider/publisher) — staying {}", state);
            return;
        }

        try {
            NxGameData gameData = ctx.gameData();
            gameData.registerSnapshotTrigger(() -> runSnapshot(ctx, p, pub));
        } catch (Throwable t) {
            // Trigger registration failures must not take down the module — the
            // initial snapshot below still publishes; only the on-demand re-publish
            // path is lost.
            log.warn("Failed to register gd-sync snapshot trigger: {}", t.getClass().getName(), t);
        }

        try {
            Executor io = ctx.io();
            io.execute(() -> runSnapshot(ctx, p, pub));
        } catch (Throwable t) {
            log.error("gd-sync initial snapshot dispatch threw {} — no snapshot published",
                    t.getClass().getName(), t);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public void onDisconnect() {
        provider = null;
        context = null;
        publisher = null;
        // Reset state so a fresh handshake re-enters the state machine cleanly without a process restart.
        state = STATE_INIT;
    }

    @Override
    public ModuleStatus currentStatus() {
        String currentState = state;
        ModuleStatus.Stats stats;
        if (STATE_ACTIVE.equals(currentState)) {
            ItemTemplateProvider p = provider;
            String entityName = p != null ? p.entityName() : ENTITY_ITEM_TEMPLATES;
            // lastSyncEpochMs stays 0 until a complete burst — lets the platform tell "never synced" from "degraded"
            EntityStats entityStats = EntityStats.builder()
                    .name(entityName)
                    .state(lastSnapshotComplete ? EntityState.HEALTHY : EntityState.DEGRADED)
                    .rowCount((long) lastSnapshotItemCount)
                    .lastSyncEpochMs(lastSyncAtEpochMs)
                    .build();
            stats = ModuleStatus.Stats.builder()
                    .entities(Collections.singletonList(entityStats))
                    .build();
        } else {
            // DISABLED / FAILED / INIT have no synced entity to report — the
            // module-level state alone tells the platform why nothing publishes.
            stats = ModuleStatus.Stats.empty();
        }
        return ModuleStatus.builder()
                .name(NAME)
                .state(currentState)
                .stats(stats)
                .build();
    }

    private void runSnapshot(ConnectContext ctx, ItemTemplateProvider p, GameDataSnapshotPublisher pub) {
        try {
            String topic = ctx.getSyncTopics().getGd().get(p.entityName());
            GameDataSnapshotPublisher.Result result =
                    pub.publishSnapshot(p, ctx.getServerId(), topic);
            if (result != null) {
                itemsPublished.addAndGet(result.count());
                lastSnapshotItemCount = result.count();
                lastSyncId = result.syncId() != null ? result.syncId().toString() : null;
                lastSnapshotComplete = result.complete();
                if (result.complete()) {
                    lastSyncAtEpochMs = System.currentTimeMillis();
                }
            } else {
                lastSnapshotComplete = false;
            }
        } catch (Throwable t) {
            // Never let a snapshot failure reach the IO pool's uncaught handler as a surprise.
            log.error("gd-sync snapshot run threw {}", t.getClass().getName(), t);
        }
    }

    long itemsPublished() {
        return itemsPublished.get();
    }

    String lastSyncId() {
        return lastSyncId;
    }

    long lastSyncAtEpochMs() {
        return lastSyncAtEpochMs;
    }

    private static List<ItemTemplateProvider> loadProviders() {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(GameDataSyncModule.class.getClassLoader());
            ServiceLoader<ItemTemplateProvider> loader = ServiceLoader.load(ItemTemplateProvider.class);
            List<ItemTemplateProvider> result = new ArrayList<ItemTemplateProvider>();
            for (ItemTemplateProvider item : loader) {
                result.add(item);
            }
            return Collections.unmodifiableList(result);
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    private static void sendViaNxKafka(ProducerRecord<byte[], Object> record, Callback callback) {
        try {
            NxKafka.instance().sendBytesKeyRecord(record, callback);
        } catch (KafkaException notConfigured) {
            log.warn("NxKafka not configured — gd-sync send dropped (topic={})", record.topic());
            invokeCallback(callback, notConfigured);
        } catch (Throwable senderFailure) {
            log.warn("NxKafka.sendBytesKeyRecord threw {} — invoking callback exceptionally",
                    senderFailure.getClass().getName());
            invokeCallback(callback,
                    senderFailure instanceof Exception ? (Exception) senderFailure
                            : new RuntimeException(senderFailure));
        }
    }

    private static void invokeCallback(Callback callback, Exception cause) {
        try {
            callback.onCompletion(null, cause);
        } catch (Throwable t) {
            log.warn("gd-sync sender callback threw {}", t.getClass().getName());
        }
    }

    private static <T> String classNamesOf(List<T> impls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < impls.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(impls.get(i).getClass().getName());
        }
        return sb.toString();
    }
}
