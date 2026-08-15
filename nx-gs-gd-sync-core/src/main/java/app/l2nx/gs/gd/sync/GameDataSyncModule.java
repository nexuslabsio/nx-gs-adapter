package app.l2nx.gs.gd.sync;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncCommand;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncResult;
import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
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
 * Tier-1 module that publishes static game-data (datapack-derived) templates onto
 * the {@code gd} sync stream. Multi-entity and data-driven: a static registry of
 * {@link EntityDescriptor}s (itemtemplate, npctemplate, skill, recipetemplate,
 * armorsettemplate, soulcrystaltemplate, classtemplate, instance, gearscore) pairs each gd
 * entity's Tier-2 SPI with its snapshot accessor and primary-key extractor, so adding an entity
 * is one registry line rather than another field / discovery block. The {@code gearscore} entity
 * is a singleton — its SPI returns {@code Optional<GearScoreRuleset>}, adapted to a 0-or-1-element
 * collection with a constant primary key so it shares the same engine. Each present provider
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
 * <p><b>Host readiness.</b> Every snapshot pass is gated on the optional
 * {@link GameDataReadinessProvider} (absent = always ready). The adapter connects during host boot,
 * before the datapack is parsed, so an ungated pass would touch providers that have nothing yet —
 * force-loading the host's parsers out of order, and letting the {@code gearscore} singleton publish
 * a {@code count=0} marker that reconcile-deletes the platform's ruleset. While the host is unready
 * the module publishes nothing and polls readiness every
 * {@link #READINESS_POLL_INTERVAL_SECONDS} seconds, so the catalogs sync even if the host never
 * calls {@code publishSnapshot()} itself.</p>
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
    private final GameDataSyncConfig config;
    private final List<GameDataReadinessProvider> readinessProviders;

    /**
     * How often the module re-asks an unready host whether its game data has loaded. The check is a
     * single boolean call, so a fixed interval beats a backoff — it costs nothing and picks the host
     * up within seconds of it finishing boot.
     */
    static final long READINESS_POLL_INTERVAL_SECONDS = 5L;

    /**
     * How long the host may stay unready before that stops being "still booting" and becomes an
     * alarm. Deliberately a separate constant from the publisher's null-snapshot grace: the two
     * happen to share a value, but tuning one must not silently retune the other.
     */
    static final long READINESS_GRACE_MS = 15L * 60L * 1000L;

    private final AtomicBoolean snapshotRunning = new AtomicBoolean(false);
    private final AtomicBoolean rerunRequested = new AtomicBoolean(false);
    private final AtomicBoolean readinessProbeFailed = new AtomicBoolean(false);

    /** Guards the scheduler lifecycle: creation, the readiness poll handle, and shutdown. */
    private final Object schedulerLock = new Object();

    private volatile String state = STATE_INIT;
    private volatile ConnectContext context;
    private volatile GameDataSnapshotPublisher publisher;
    private volatile List<EntitySync<?>> entitySyncs = Collections.emptyList();
    private volatile GameDataReadinessProvider readiness;
    private volatile EscalationTracker readinessTracker;

    // @GuardedBy("schedulerLock")
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

    /**
     * @param readinessProviders explicit readiness-provider list, or {@code null} to discover them
     *                           via {@link ServiceLoader} as production does. The explicit form
     *                           exists because ServiceLoader cannot express "none registered" or
     *                           "two registered" on a classpath shared by the whole test module.
     */
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

    /**
     * The gd entities this module drives — one descriptor per Tier-2 provider SPI. Order
     * is the snapshot/heartbeat order; each entry is independent.
     */
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
        // Singleton entity: the SPI returns Optional<GearScoreRuleset>, adapted to the collection
        // engine as a 0-or-1-element list so it reuses the same burst / SNAPSHOT_COMPLETE flow.
        // Empty (gear score disabled) → empty collection → legal count=0 snapshot whose stale-delete
        // drops the singleton row. Constant pk — a singleton has no numeric primary key.
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

    /**
     * Whether the host says its game-data catalogs are loaded. No provider registered means the host
     * predates the SPI and is treated as always ready. A provider that throws counts as NOT ready:
     * publishing on a broken readiness signal risks an empty snapshot reconcile-deleting a catalog,
     * while refusing only delays the burst until the next poll.
     */
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
            // Registration failure must not take down the module — the snapshot
            // burst still publishes on connect / host reload / schedule; only the
            // manual remote-resync RPC surface is lost.
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
            // Trigger registration failures must not take down the module — the
            // initial snapshot below still publishes; only the on-demand re-publish
            // path is lost.
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

    /**
     * Re-check host readiness until it flips, then publish once and stop polling. Makes the module
     * self-sufficient: a host that never calls {@code publishSnapshot()} still gets its catalogs
     * synced, and one that does simply beats the poller to it.
     */
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

    // package-visible for unit tests; production callers go through startReadinessPolling()
    void pollReadinessOnce(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        if (hostReady()) {
            // runAllSnapshots cancels the poll itself once its gate opens — going through the
            // dispatch keeps a single place responsible for that.
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

    // package-visible for unit tests — proves the fallback poll is disarmed once a pass publishes
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

    /**
     * One scheduler serves both readiness polling and the periodic resync — they never run long
     * enough to block each other, and a second daemon thread per connection buys nothing. Returns
     * {@code null} once the module has been shut down: {@code start()} runs on the adapter's connect
     * thread while {@code stop()} runs on the host's, so a late scheduling attempt must not resurrect
     * a daemon that would outlive the connection.
     */
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

    /**
     * Guarded full-snapshot pass. Coalesces concurrent triggers (connect,
     * host datapack-reload, remote resync, scheduler) into a single in-flight
     * runner. Every caller records its intent ({@code rerunRequested=true})
     * <em>before</em> contending for the running flag, so no trigger is ever
     * lost: a caller that cannot acquire the flag has already armed a rerun the
     * active runner will observe; the runner clears the flag at the start of
     * each pass and re-loops while it is set, including across the flag-release
     * window (re-acquired by the outer loop), so a request landing in the tail
     * of a pass still fires exactly one more pass.
     */
    private void runAllSnapshots(ConnectContext ctx, GameDataSnapshotPublisher pub) {
        // A task queued on a previous connection's io() must not publish against the serverId and
        // topic map of the connection that replaced it.
        if (!STATE_ACTIVE.equals(state) || ctx != context) {
            return;
        }
        // Gate the whole pass, not each entity: the providers must not be touched at all while the
        // host is loading (reading them force-loads its parsers out of order), and the gearscore
        // singleton cannot express "not ready" — its Optional.empty() would publish a legal
        // count=0 marker and reconcile-delete the platform's ruleset.
        if (!hostReady()) {
            // Also covers a host that goes unready again (datapack reload) after a successful pass:
            // without re-arming, nothing would ever publish again.
            startReadinessPolling(ctx, pub);
            return;
        }
        // Whoever opens the gate first — the host's own publishSnapshot() or the fallback poll —
        // makes the poll redundant. Cancelling here rather than in the poll is what stops a boot
        // from publishing the whole catalog twice.
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
