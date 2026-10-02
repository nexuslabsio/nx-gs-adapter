package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncCommand;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncResult;
import app.l2nx.gs.adapter.api.kafka.ops.model.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.model.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStates;
import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate.ArmorSetTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate.ClassTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.gearscore.GearScoreRuleset;
import app.l2nx.gs.adapter.api.kafka.sync.gd.instancetemplate.InstanceTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate.NpcTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.recipetemplate.RecipeTemplate;
import app.l2nx.gs.adapter.api.kafka.sync.gd.skill.Skill;
import app.l2nx.gs.adapter.api.kafka.sync.gd.soulcrystaltemplate.SoulCrystalTemplate;
import app.l2nx.gs.adapter.api.spi.*;
import app.l2nx.gs.adapter.api.spi.capability.NxGameData;
import app.l2nx.gs.adapter.api.spi.provider.ArmorSetTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.ClassTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.GameDataReadinessProvider;
import app.l2nx.gs.adapter.api.spi.provider.GearScoreRulesetProvider;
import app.l2nx.gs.adapter.api.spi.provider.InstanceTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.ItemTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.NpcTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.RecipeTemplateProvider;
import app.l2nx.gs.adapter.api.spi.provider.SkillProvider;
import app.l2nx.gs.adapter.api.spi.provider.SoulCrystalTemplateProvider;
import app.l2nx.gs.commons.concurrent.DaemonThreadFactory;
import app.l2nx.gs.commons.concurrent.SafeRunnable;
import app.l2nx.gs.kafka.KafkaException;
import app.l2nx.gs.kafka.NxKafka;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Publishes static game-data templates onto the {@code gd} sync stream, one independent {@link EntitySync} per
 * present provider SPI. Every pass is gated on the optional {@link GameDataReadinessProvider}: the adapter
 * connects before the datapack is parsed, and an ungated pass would force-load host parsers out of order and
 * let the {@code gearscore} singleton publish a {@code count=0} marker that reconcile-deletes the ruleset.
 * Hooks catch {@link Throwable} and never propagate to the host JVM.
 */
public final class GameDataSyncModule implements AdapterModule {

    private static final NxLog log = NxLogFactory.getLogger(GameDataSyncModule.class);

    static final String NAME = "gd-sync";

    static final String STATE_INIT = ModuleStates.INIT;
    static final String STATE_DISABLED = ModuleStates.DISABLED;
    static final String STATE_FAILED = ModuleStates.FAILED;
    static final String STATE_ACTIVE = ModuleStates.ACTIVE;

    private final List<EntityDescriptor<?, ?>> descriptors;
    private final GameDataSender sender;
    private final GameDataSyncConfig config;
    private final List<GameDataReadinessProvider> readinessProviders;

    static final long READINESS_POLL_INTERVAL_SECONDS = 5L;

    /** Deliberately separate from the publisher's null-snapshot grace: they share a value by coincidence. */
    static final long READINESS_GRACE_MS = 15L * 60L * 1000L;

    private final AtomicBoolean snapshotRunning = new AtomicBoolean(false);
    private final AtomicBoolean rerunRequested = new AtomicBoolean(false);
    private final AtomicBoolean readinessProbeFailed = new AtomicBoolean(false);

    private final Object schedulerLock = new Object();

    private volatile String state = STATE_INIT;
    private volatile ConnectContext context;
    private volatile GameDataSnapshotPublisher publisher;
    private volatile List<EntitySync<?>> entitySyncs = Collections.emptyList();
    private volatile GameDataReadinessProvider readiness;
    private volatile EscalationTracker readinessTracker;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> readinessPoll;
    private boolean schedulerShutdown;

    public GameDataSyncModule() {
        this(defaultDescriptors(), GameDataSyncModule::sendViaNxKafka, GameDataSyncConfig.fromProductionChain());
    }

    GameDataSyncModule(List<EntityDescriptor<?, ?>> descriptors, GameDataSender sender) {
        this(descriptors, sender, GameDataSyncConfig.defaults());
    }

    GameDataSyncModule(List<EntityDescriptor<?, ?>> descriptors, GameDataSender sender, GameDataSyncConfig config) {
        this(descriptors, sender, config, null);
    }

    /** {@code readinessProviders == null} means discover via ServiceLoader; explicit lists let tests express "none"/"two". */
    GameDataSyncModule(
            List<EntityDescriptor<?, ?>> descriptors,
            GameDataSender sender,
            GameDataSyncConfig config,
            List<GameDataReadinessProvider> readinessProviders) {
        this.descriptors = descriptors;
        this.sender = sender;
        this.config = config;
        this.readinessProviders = readinessProviders;
    }

    /** Order is the snapshot/heartbeat order. */
    static List<EntityDescriptor<?, ?>> defaultDescriptors() {
        List<EntityDescriptor<?, ?>> list = new ArrayList<EntityDescriptor<?, ?>>();
        list.add(new EntityDescriptor<ItemTemplateProvider, ItemTemplate>(
                ItemTemplateProvider.class, ItemTemplateProvider::entityName, ItemTemplateProvider::snapshot, t ->
                        (long) t.getId()));
        list.add(new EntityDescriptor<NpcTemplateProvider, NpcTemplate>(
                NpcTemplateProvider.class, NpcTemplateProvider::entityName, NpcTemplateProvider::snapshot, t ->
                        (long) t.getId()));
        list.add(new EntityDescriptor<SkillProvider, Skill>(
                SkillProvider.class, SkillProvider::entityName, SkillProvider::snapshot, t -> (long) t.getId()));
        list.add(new EntityDescriptor<RecipeTemplateProvider, RecipeTemplate>(
                RecipeTemplateProvider.class, RecipeTemplateProvider::entityName, RecipeTemplateProvider::snapshot, t ->
                        (long) t.getId()));
        list.add(new EntityDescriptor<ArmorSetTemplateProvider, ArmorSetTemplate>(
                ArmorSetTemplateProvider.class,
                ArmorSetTemplateProvider::entityName,
                ArmorSetTemplateProvider::snapshot,
                t -> (long) t.getId()));
        list.add(new EntityDescriptor<SoulCrystalTemplateProvider, SoulCrystalTemplate>(
                SoulCrystalTemplateProvider.class,
                SoulCrystalTemplateProvider::entityName,
                SoulCrystalTemplateProvider::snapshot,
                t -> (long) t.getId()));
        list.add(new EntityDescriptor<ClassTemplateProvider, ClassTemplate>(
                ClassTemplateProvider.class,
                ClassTemplateProvider::entityName,
                ClassTemplateProvider::snapshot,
                t -> t.getClazz() == null ? -1L : t.getClazz().ordinal()));
        list.add(new EntityDescriptor<InstanceTemplateProvider, InstanceTemplate>(
                InstanceTemplateProvider.class,
                InstanceTemplateProvider::entityName,
                InstanceTemplateProvider::snapshot,
                t -> (long) t.getId()));
        // Singleton: Optional adapted to a 0-or-1 list; empty is a legal count=0 snapshot that deletes the row.
        list.add(new EntityDescriptor<GearScoreRulesetProvider, GearScoreRuleset>(
                GearScoreRulesetProvider.class,
                GearScoreRulesetProvider::entityName,
                p -> p.snapshot().map(Collections::singletonList).orElse(Collections.<GearScoreRuleset>emptyList()),
                t -> 0L));
        return Collections.unmodifiableList(list);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void onConnect(ConnectContext ctx) {
        this.context = ctx;
        if (ctx == null
                || ctx.getSyncTopics() == null
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
                    + "ItemTemplateProvider, NpcTemplateProvider, SkillProvider and/or one of the "
                    + "recipe/armor-set/soul-crystal/class/instance providers via META-INF/services to "
                    + "enable game-data sync.");
            state = STATE_DISABLED;
            return;
        }

        List<GameDataReadinessProvider> discovered =
                readinessProviders != null ? readinessProviders : loadProviders(GameDataReadinessProvider.class);
        if (discovered.size() > 1) {
            log.error(
                    "Multiple GameDataReadinessProvider impls on classpath: [{}]. gd-sync FAILED.",
                    classNamesOf(discovered));
            state = STATE_FAILED;
            return;
        }
        this.readiness = discovered.isEmpty() ? null : discovered.get(0);
        this.readinessTracker = new EscalationTracker(READINESS_GRACE_MS, System::currentTimeMillis);
        this.readinessProbeFailed.set(false);
        resetSchedulerShutdown();
        if (readiness != null) {
            log.info(
                    "GameDataReadinessProvider resolved: {}",
                    readiness.getClass().getName());
        }

        this.entitySyncs = Collections.unmodifiableList(resolved);
        this.publisher = new GameDataSnapshotPublisher(sender);
        registerResyncHandler(ctx);
        state = STATE_ACTIVE;
    }

    /** No provider = always ready; a throwing provider = NOT ready, since a broken signal risks a reconcile-delete. */
    private boolean hostReady() {
        GameDataReadinessProvider provider = readiness;
        if (provider == null) {
            return true;
        }
        try {
            boolean ready = provider.ready();
            readinessProbeFailed.compareAndSet(true, false);
            return ready;
        } catch (Throwable t) {
            if (readinessProbeFailed.compareAndSet(false, true)) {
                log.warn(
                        "GameDataReadinessProvider {} threw {} — treating the host as not ready",
                        provider.getClass().getName(),
                        t.getClass().getName(),
                        t);
            }
            return false;
        }
    }

    private void registerResyncHandler(ConnectContext ctx) {
        try {
            ctx.commands().on(GdResyncCommand.class, this::handleGdResync);
        } catch (Throwable t) {
            // only the remote-resync RPC is lost; snapshots still publish
            log.warn(
                    "Failed to register gd-sync resync command handler: {}",
                    t.getClass().getName(),
                    t);
        }
    }

    CommandResult<GdResyncResult> handleGdResync(GdResyncCommand cmd, CommandContext cctx) {
        final ConnectContext ctx = context;
        final GameDataSnapshotPublisher pub = publisher;
        List<EntitySync<?>> syncs = entitySyncs;
        if (!STATE_ACTIVE.equals(state) || ctx == null || pub == null || syncs.isEmpty()) {
            return CommandResult.unavailable("gd-sync module is not active");
        }
        if (!hostReady()) {
            // Acking with acceptedEntities and then publishing nothing would leave the platform
            // waiting for a snapshot that was never scheduled.
            return CommandResult.unavailable("gd-sync host game data is not ready yet");
        }
        List<String> entityNames = registeredEntityNames(syncs);
        try {
            ctx.io().execute(() -> runAllSnapshots(ctx, pub));
        } catch (Throwable t) {
            log.error(
                    "gd-sync resync dispatch threw {} — no snapshot scheduled",
                    t.getClass().getName(),
                    t);
            return CommandResult.unavailable("gd-sync could not schedule the snapshot");
        }
        return CommandResult.ok(
                GdResyncResult.builder().acceptedEntities(entityNames).build());
    }

    private static List<String> registeredEntityNames(List<EntitySync<?>> syncs) {
        List<String> names = new ArrayList<String>(syncs.size());
        for (EntitySync<?> sync : syncs) {
            names.add(sync.entityName());
        }
        return names;
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
            // only the on-demand re-publish is lost
            log.warn(
                    "Failed to register gd-sync snapshot trigger: {}",
                    t.getClass().getName(),
                    t);
        }

        if (hostReady()) {
            dispatchSnapshot(ctx, pub);
        } else {
            log.info("gd-sync: host game data not ready — deferring the initial snapshot");
            startReadinessPolling(ctx, pub);
        }

        startResyncScheduler(ctx, pub);
    }

    private void dispatchSnapshot(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        try {
            Executor io = ctx.io();
            io.execute(() -> runAllSnapshots(ctx, pub));
        } catch (Throwable t) {
            log.error(
                    "gd-sync snapshot dispatch threw {} — no snapshot published",
                    t.getClass().getName(),
                    t);
        }
    }

    /** Fallback for hosts that never call {@code publishSnapshot()} themselves. */
    private void startReadinessPolling(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        synchronized (schedulerLock) {
            if (readinessPoll != null) {
                return;
            }
            ScheduledExecutorService exec = ensureScheduler();
            if (exec == null) {
                return;
            }
            Runnable probe = SafeRunnable.wrap(() -> pollReadinessOnce(ctx, pub), log);
            readinessPoll = exec.scheduleWithFixedDelay(
                    probe, READINESS_POLL_INTERVAL_SECONDS, READINESS_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    void pollReadinessOnce(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        if (hostReady()) {
            dispatchSnapshot(ctx, pub);
            return;
        }
        EscalationTracker tracker = readinessTracker;
        if (tracker != null && tracker.observe() == EscalationTracker.Stage.ESCALATED) {
            log.error(
                    "gd-sync: host game data still not ready after {} minutes — no catalog will sync "
                            + "until the host reports ready",
                    READINESS_GRACE_MS / 60000L);
        }
    }

    private void cancelReadinessPolling() {
        synchronized (schedulerLock) {
            ScheduledFuture<?> poll = readinessPoll;
            readinessPoll = null;
            if (poll != null) {
                poll.cancel(false);
            }
        }
    }

    boolean readinessPollArmed() {
        synchronized (schedulerLock) {
            return readinessPoll != null;
        }
    }

    private void startResyncScheduler(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        if (!config.scheduledResyncEnabled()) {
            return;
        }
        final int hours = config.resyncIntervalHours();
        Runnable tick = SafeRunnable.wrap(() -> runAllSnapshots(ctx, pub), log);
        synchronized (schedulerLock) {
            ScheduledExecutorService exec = ensureScheduler();
            if (exec == null) {
                return;
            }
            exec.scheduleWithFixedDelay(tick, hours, hours, TimeUnit.HOURS);
        }
        log.info("gd-sync scheduled resync enabled — every {}h", hours);
    }

    /** Returns {@code null} after shutdown: start() and stop() race on different threads, and a late call must not resurrect the daemon. */
    private ScheduledExecutorService ensureScheduler() {
        assert Thread.holdsLock(schedulerLock);
        if (schedulerShutdown) {
            return null;
        }
        if (scheduler == null) {
            scheduler =
                    Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.named("nx-gd-sync-scheduler", log));
        }
        return scheduler;
    }

    private void shutdownScheduler() {
        ScheduledExecutorService exec;
        synchronized (schedulerLock) {
            schedulerShutdown = true;
            ScheduledFuture<?> poll = readinessPoll;
            readinessPoll = null;
            if (poll != null) {
                poll.cancel(false);
            }
            exec = scheduler;
            scheduler = null;
        }
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    private void resetSchedulerShutdown() {
        synchronized (schedulerLock) {
            schedulerShutdown = false;
        }
    }

    @Override
    public void stop() {
        shutdownScheduler();
    }

    @Override
    public void onDisconnect() {
        shutdownScheduler();
        readiness = null;
        readinessTracker = null;
        entitySyncs = Collections.emptyList();
        context = null;
        publisher = null;
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
            stats = ModuleStatus.Stats.empty();
        }
        return ModuleStatus.builder()
                .name(NAME)
                .state(currentState)
                .stats(stats)
                .build();
    }

    /**
     * Coalesces concurrent triggers into one in-flight runner. Callers set {@code rerunRequested} before
     * contending for the running flag, so a trigger landing in the tail of a pass is never lost.
     */
    private void runAllSnapshots(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        // stale task from a previous connection must not publish against the new one's serverId/topics
        if (!STATE_ACTIVE.equals(state) || ctx != context) {
            return;
        }
        // gate the whole pass: gearscore's Optional.empty() cannot express "not ready"
        if (!hostReady()) {
            // re-arms after a host goes unready again (datapack reload)
            startReadinessPolling(ctx, pub);
            return;
        }
        // cancelled here, not in the poll, so a boot cannot publish the whole catalog twice
        cancelReadinessPolling();
        rerunRequested.set(true);
        while (snapshotRunning.compareAndSet(false, true)) {
            try {
                do {
                    rerunRequested.set(false);
                    for (EntitySync<?> sync : entitySyncs) {
                        sync.run(pub, ctx);
                    }
                } while (rerunRequested.get() && hostReady());
            } finally {
                snapshotRunning.set(false);
            }
            if (!rerunRequested.get()) {
                return;
            }
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
            log.warn(
                    "NxKafka.sendBytesKeyRecord threw {} — invoking callback exceptionally",
                    senderFailure.getClass().getName());
            invokeCallback(
                    callback,
                    senderFailure instanceof Exception
                            ? (Exception) senderFailure
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

    static final class EntityDescriptor<P, T> {

        private final Class<P> spi;
        private final Function<P, String> entityNameFn;
        private final Function<P, Collection<T>> snapshotFn;
        private final ToLongFunction<T> pkFn;

        EntityDescriptor(
                Class<P> spi,
                Function<P, String> entityNameFn,
                Function<P, Collection<T>> snapshotFn,
                ToLongFunction<T> pkFn) {
            this.spi = spi;
            this.entityNameFn = entityNameFn;
            this.snapshotFn = snapshotFn;
            this.pkFn = pkFn;
        }

        /** Returns {@code null} when no provider is registered; more than one is ambiguous and throws. */
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
                    log.error(
                            "gd-sync provider for entity '{}' threw {} pulling snapshot — burst aborted",
                            entityName,
                            t.getClass().getName(),
                            t);
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
                log.error(
                        "gd-sync snapshot run for entity '{}' threw {}",
                        entityName,
                        t.getClass().getName(),
                        t);
            }
        }

        EntityStats toStats() {
            // lastSyncEpochMs stays 0 until a complete burst: "never synced" vs "degraded"
            return EntityStats.builder()
                    .name(entityName)
                    .state(lastSnapshotComplete ? EntityState.HEALTHY : EntityState.DEGRADED)
                    .rowCount((long) lastSnapshotItemCount)
                    .lastSyncEpochMs(lastSyncAtEpochMs)
                    .build();
        }
    }
}
