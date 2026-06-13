package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate.ArmorSetTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate.ClassTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate.NpcTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.recipetemplate.RecipeTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate.SkillTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.soulcrystaltemplate.SoulCrystalTemplate;
import app.l2nx.gs.adapter.api.spi.*;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Tier-1 module that publishes static game-data (datapack-derived) templates onto
 * the {@code gd} sync stream. Multi-entity and data-driven: a static registry of
 * {@link EntityDescriptor}s (itemtemplate, npctemplate, skilltemplate, recipetemplate,
 * armorsettemplate, soulcrystaltemplate, classtemplate) pairs each gd entity's
 * Tier-2 SPI with its snapshot accessor and primary-key extractor, so adding an entity is
 * one registry line rather than another field / discovery block. Each present provider
 * becomes an independent {@link EntitySync} with its own snapshot burst, {@code syncId}
 * and heartbeat {@link EntityStats}. Reads its inputs in order:
 *
 * <ol>
 *     <li>{@code ctx.getSyncTopics().getGd()} — empty/null → DISABLED + WARN.</li>
 *     <li>Each descriptor's Tier-2 SPI via {@link ServiceLoader} — &gt;1 of a kind →
 *     FAILED; none of any → DISABLED; otherwise each present provider becomes an active
 *     entity sync.</li>
 * </ol>
 *
 * <p>On {@link #start()} (when ACTIVE) the module registers a snapshot trigger on
 * {@code ctx.gameData()} — so host code that calls
 * {@code ctx.gameData().publishSnapshot()} (e.g. after an in-game datapack reload)
 * re-publishes a fresh full snapshot of every entity — and fires the initial
 * snapshot once. Both run on {@code ctx.io()} so they never block the connect / game
 * thread.</p>
 *
 * <p>Exception-safe throughout: every hook catches {@link Throwable} and never
 * propagates to the host JVM. A failure publishing one entity never aborts another.</p>
 */
public final class GameDataSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSyncModule.class);

    static final String NAME = "gd-sync";

    static final String STATE_INIT = "INIT";
    static final String STATE_DISABLED = "DISABLED";
    static final String STATE_FAILED = "FAILED";
    static final String STATE_ACTIVE = "ACTIVE";

    private final List<EntityDescriptor<?, ?>> descriptors;
    private final GameDataSender sender;

    private volatile String state = STATE_INIT;
    private volatile ConnectContext context;
    private volatile GameDataSnapshotPublisher publisher;
    private volatile List<EntitySync<?>> entitySyncs = Collections.emptyList();

    public GameDataSyncModule() {
        this(defaultDescriptors(), GameDataSyncModule::sendViaNxKafka);
    }

    GameDataSyncModule(List<EntityDescriptor<?, ?>> descriptors, GameDataSender sender) {
        this.descriptors = descriptors;
        this.sender = sender;
    }

    /**
     * The gd entities this module drives — one descriptor per Tier-2 provider SPI. Order
     * is the snapshot/heartbeat order; each entry is independent.
     */
    static List<EntityDescriptor<?, ?>> defaultDescriptors() {
        List<EntityDescriptor<?, ?>> list = new ArrayList<EntityDescriptor<?, ?>>();
        list.add(new EntityDescriptor<ItemTemplateProvider, ItemTemplate>(ItemTemplateProvider.class,
                ItemTemplateProvider::entityName, ItemTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<NpcTemplateProvider, NpcTemplate>(NpcTemplateProvider.class,
                NpcTemplateProvider::entityName, NpcTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<SkillTemplateProvider, SkillTemplate>(SkillTemplateProvider.class,
                SkillTemplateProvider::entityName, SkillTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<RecipeTemplateProvider, RecipeTemplate>(RecipeTemplateProvider.class,
                RecipeTemplateProvider::entityName, RecipeTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<ArmorSetTemplateProvider, ArmorSetTemplate>(ArmorSetTemplateProvider.class,
                ArmorSetTemplateProvider::entityName, ArmorSetTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<SoulCrystalTemplateProvider, SoulCrystalTemplate>(SoulCrystalTemplateProvider.class,
                SoulCrystalTemplateProvider::entityName, SoulCrystalTemplateProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<ClassTemplateProvider, ClassTemplate>(ClassTemplateProvider.class,
                ClassTemplateProvider::entityName, ClassTemplateProvider::snapshot,
                t -> t.getClazz() == null ? -1L : t.getClazz().ordinal()));
        return Collections.unmodifiableList(list);
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

        List<EntitySync<?>> resolved = new ArrayList<EntitySync<?>>(descriptors.size());
        for (EntityDescriptor<?, ?> d : descriptors) {
            EntitySync<?> sync;
            try {
                sync = d.resolve();
            } catch (DuplicateProviderException dup) {
                log.error("Multiple {} impls on classpath: [{}]. gd-sync FAILED.", dup.spiName, dup.implNames);
                state = STATE_FAILED;
                return;
            }
            if (sync != null) {
                log.info("{} resolved: entity={}", d.spiName(), sync.entityName());
                resolved.add(sync);
            }
        }
        if (resolved.isEmpty()) {
            log.warn("No gd template provider SPI registered — gd-sync DISABLED. Register an "
                    + "ItemTemplateProvider, NpcTemplateProvider, SkillTemplateProvider and/or one of the "
                    + "recipe/armor-set/soul-crystal/class providers via META-INF/services to "
                    + "enable game-data sync.");
            state = STATE_DISABLED;
            return;
        }

        this.entitySyncs = Collections.unmodifiableList(resolved);
        this.publisher = new GameDataSnapshotPublisher(sender);
        state = STATE_ACTIVE;
    }

    @Override
    public void start() {
        if (!STATE_ACTIVE.equals(state)) {
            return;
        }
        final ConnectContext ctx = context;
        final GameDataSnapshotPublisher pub = publisher;
        if (ctx == null || pub == null) {
            log.error("gd-sync.start: missing dependency (context/publisher) — staying {}", state);
            return;
        }

        try {
            NxGameData gameData = ctx.gameData();
            gameData.registerSnapshotTrigger(() -> runAllSnapshots(ctx, pub));
        } catch (Throwable t) {
            // Trigger registration failures must not take down the module — the
            // initial snapshot below still publishes; only the on-demand re-publish
            // path is lost.
            log.warn("Failed to register gd-sync snapshot trigger: {}", t.getClass().getName(), t);
        }

        try {
            Executor io = ctx.io();
            io.execute(() -> runAllSnapshots(ctx, pub));
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
        entitySyncs = Collections.emptyList();
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
            List<EntitySync<?>> syncs = entitySyncs;
            List<EntityStats> entities = new ArrayList<EntityStats>(syncs.size());
            for (EntitySync<?> sync : syncs) {
                entities.add(sync.toStats());
            }
            stats = ModuleStatus.Stats.builder()
                    .entities(Collections.unmodifiableList(entities))
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

    private void runAllSnapshots(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        for (EntitySync<?> sync : entitySyncs) {
            sync.run(pub, ctx);
        }
    }

    private static <T> List<T> loadProviders(Class<T> spi) {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(GameDataSyncModule.class.getClassLoader());
            ServiceLoader<T> loader = ServiceLoader.load(spi);
            List<T> result = new ArrayList<T>();
            for (T item : loader) {
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

    /**
     * Static registration of one gd entity: its Tier-2 provider SPI plus the functions to
     * read the entity name, pull a snapshot, and extract a template's primary key. Erases
     * the provider/template types behind {@link #resolve()} so the module holds a uniform
     * {@code List<EntityDescriptor<?, ?>>}.
     */
    static final class EntityDescriptor<P, T> {

        private final Class<P> spi;
        private final Function<P, String> entityNameFn;
        private final Function<P, Collection<T>> snapshotFn;
        private final ToLongFunction<T> pkFn;

        EntityDescriptor(Class<P> spi, Function<P, String> entityNameFn,
                         Function<P, Collection<T>> snapshotFn, ToLongFunction<T> pkFn) {
            this.spi = spi;
            this.entityNameFn = entityNameFn;
            this.snapshotFn = snapshotFn;
            this.pkFn = pkFn;
        }

        /**
         * Resolve the single registered provider into an {@link EntitySync}, or {@code null}
         * when none is on the classpath. Throws {@link DuplicateProviderException} when more
         * than one impl of the SPI is present (ambiguous — the module fails rather than
         * guessing).
         */
        EntitySync<T> resolve() {
            List<P> providers = loadProviders(spi);
            if (providers.size() > 1) {
                throw new DuplicateProviderException(spi.getSimpleName(), classNamesOf(providers));
            }
            if (providers.isEmpty()) {
                return null;
            }
            final P provider = providers.get(0);
            return new EntitySync<T>(entityNameFn.apply(provider), () -> snapshotFn.apply(provider), pkFn);
        }

        String spiName() {
            return spi.getSimpleName();
        }
    }

    private static final class DuplicateProviderException extends RuntimeException {
        final String spiName;
        final String implNames;

        DuplicateProviderException(String spiName, String implNames) {
            super("Multiple " + spiName + " impls: " + implNames);
            this.spiName = spiName;
            this.implNames = implNames;
        }
    }

    /**
     * Per-entity snapshot handle — owns the snapshot pull, publish, and heartbeat
     * stats for one gd entity. Generic over the template type so every entity shares
     * one implementation.
     */
    private static final class EntitySync<T> {

        private final String entityName;
        private final Supplier<Collection<T>> snapshotSupplier;
        private final ToLongFunction<T> pkOf;

        private final AtomicLong published = new AtomicLong();
        private volatile long lastSyncAtEpochMs;
        private volatile int lastSnapshotItemCount;
        private volatile boolean lastSnapshotComplete;

        EntitySync(String entityName, Supplier<Collection<T>> snapshotSupplier, ToLongFunction<T> pkOf) {
            this.entityName = entityName;
            this.snapshotSupplier = snapshotSupplier;
            this.pkOf = pkOf;
        }

        String entityName() {
            return entityName;
        }

        void run(GameDataSnapshotPublisher pub, ConnectContext ctx) {
            try {
                String topic = ctx.getSyncTopics().getGd().get(entityName);
                Collection<T> items;
                try {
                    items = snapshotSupplier.get();
                } catch (Throwable t) {
                    log.error("gd-sync provider for entity '{}' threw {} pulling snapshot — burst aborted",
                            entityName, t.getClass().getName(), t);
                    lastSnapshotComplete = false;
                    return;
                }
                GameDataSnapshotPublisher.Result result =
                        pub.publishSnapshot(entityName, items, pkOf, ctx.getServerId(), topic);
                if (result != null) {
                    published.addAndGet(result.count());
                    lastSnapshotItemCount = result.count();
                    lastSnapshotComplete = result.complete();
                    if (result.complete()) {
                        lastSyncAtEpochMs = System.currentTimeMillis();
                    }
                } else {
                    lastSnapshotComplete = false;
                }
            } catch (Throwable t) {
                log.error("gd-sync snapshot run for entity '{}' threw {}", entityName, t.getClass().getName(), t);
            }
        }

        EntityStats toStats() {
            // lastSyncEpochMs stays 0 until a complete burst — lets the platform tell "never synced" from "degraded"
            return EntityStats.builder()
                    .name(entityName)
                    .state(lastSnapshotComplete ? EntityState.HEALTHY : EntityState.DEGRADED)
                    .rowCount((long) lastSnapshotItemCount)
                    .lastSyncEpochMs(lastSyncAtEpochMs)
                    .build();
        }
    }
}
