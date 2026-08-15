package app.l2nx.gs.gd.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.domain.item.ItemClass;
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.CommandStatus;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncCommand;
import app.l2nx.gs.adapter.api.kafka.commands.gd.GdResyncResult;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemTemplate;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.GameDataReadinessProvider;
import app.l2nx.gs.adapter.api.spi.NxGameData;
import app.l2nx.gs.adapter.api.spi.NxGameDataTrigger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link GameDataSyncModule}'s host-readiness gate through the production
 * {@link GameDataSyncModule#defaultDescriptors()} registry, resolving both providers via a real
 * {@link java.util.ServiceLoader}: {@link TestItemTemplateProvider} (the gated Tier-2 provider,
 * registered under {@code META-INF/services} like {@link TestGearScoreRulesetProvider}) and
 * {@link TestGameDataReadinessProvider} (the Tier-2 readiness signal under test).
 */
class GameDataReadinessTest {

    private static final String ITEM_TOPIC = "kbt.gd.sync.itemtemplate";
    private static final String GEARSCORE_TOPIC = "kbt.gd.sync.gearscore";

    private List<GameDataSyncEvent<?>> recorded;
    private GameDataSender sender;
    private GameDataSyncModule module;
    private ConnectContext ctx;
    private CapturingGameData gameData;

    @BeforeEach
    void setUp() {
        recorded = new ArrayList<GameDataSyncEvent<?>>();
        sender = new RecordingSender(recorded);
        module = new GameDataSyncModule(GameDataSyncModule.defaultDescriptors(), sender);
        gameData = new CapturingGameData();

        Map<String, String> gdTopics = new HashMap<String, String>();
        gdTopics.put("itemtemplate", ITEM_TOPIC);
        gdTopics.put("gearscore", GEARSCORE_TOPIC);
        SyncTopics topics = SyncTopics.builder().gd(gdTopics).build();
        ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("kbt")
                .serverId(UUID.randomUUID())
                .serverSlug("kbt-x1")
                .serverName("x1")
                .adapterVersion("test")
                .syncTopics(topics)
                .gameData(gameData)
                .build();
    }

    @AfterEach
    void tearDown() {
        // Any test that called start() with an unready host arms a real 5s daemon poll — leaving
        // it running would fire into a finished test and mutate shared state after the fact.
        module.onDisconnect();
        TestGameDataReadinessProvider.ready = true;
        TestItemTemplateProvider.snapshot = Collections.emptyList();
        TestItemTemplateProvider.callCount.set(0);
        TestGearScoreRulesetProvider.snapshot = Optional.empty();
    }

    private static ItemTemplate itemTemplate(int id) {
        return ItemTemplate.builder().id(id).type(ItemClass.ETC).build();
    }

    private List<GameDataSyncEvent<?>> eventsFor(String entity) {
        List<GameDataSyncEvent<?>> out = new ArrayList<GameDataSyncEvent<?>>();
        for (GameDataSyncEvent<?> e : recorded) {
            if (entity.equals(e.getEntityName())) {
                out.add(e);
            }
        }
        return out;
    }

    @Nested
    class WhileUnready {

        @Test
        void start_shouldPublishNothing_whileHostUnready() {
            TestGameDataReadinessProvider.ready = false;
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));

            module.onConnect(ctx);
            module.start();

            assertTrue(
                    recorded.isEmpty(),
                    "no burst at all while unready — specifically no itemtemplate SNAPSHOT_COMPLETE count=0, "
                            + "the data-loss case this feature exists to stop");
        }

        @Test
        void start_shouldNotConsultTier2Provider_whileHostUnready() {
            TestGameDataReadinessProvider.ready = false;
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));

            module.onConnect(ctx);
            module.start();

            assertEquals(0, TestItemTemplateProvider.callCount.get());
        }

        @Test
        void handleGdResync_shouldReturnUnavailable_whileHostUnready() {
            TestGameDataReadinessProvider.ready = false;
            module.onConnect(ctx);
            module.start();

            CommandResult<GdResyncResult> result =
                    module.handleGdResync(GdResyncCommand.builder().build(), null);

            assertEquals(CommandStatus.UNAVAILABLE, result.getStatus());
        }
    }

    @Nested
    class OnceReady {

        @Test
        void pollReadinessOnce_shouldPublishFullBurst_onceHostBecomesReady() {
            TestGameDataReadinessProvider.ready = false;
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));

            module.onConnect(ctx);
            module.start();
            assertTrue(recorded.isEmpty(), "deferred while unready");

            TestGameDataReadinessProvider.ready = true;
            module.pollReadinessOnce(ctx, new GameDataSnapshotPublisher(sender));

            List<GameDataSyncEvent<?>> itemEvents = eventsFor("itemtemplate");
            assertEquals(2, itemEvents.size(), "one UPSERT + one SNAPSHOT_COMPLETE");
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, itemEvents.get(0).getOp());
            assertEquals(
                    GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE,
                    itemEvents.get(1).getOp());
            assertEquals(Integer.valueOf(1), itemEvents.get(1).getCount());
        }

        @Test
        void start_shouldPublishFullBurst_asBeforeTheReadinessFeature() {
            // TestGameDataReadinessProvider.ready defaults to true — this is the regression
            // case: a host that reports ready from the start behaves exactly as it did before
            // the readiness gate existed (see GearScoreDescriptorTest).
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));

            module.onConnect(ctx);
            module.start();

            List<GameDataSyncEvent<?>> itemEvents = eventsFor("itemtemplate");
            assertEquals(2, itemEvents.size(), "one UPSERT + one SNAPSHOT_COMPLETE");
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, itemEvents.get(0).getOp());
            assertEquals(
                    GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE,
                    itemEvents.get(1).getOp());
            assertEquals(Integer.valueOf(1), itemEvents.get(1).getCount());
        }

        @Test
        void handleGdResync_shouldReturnOk_whenHostReady() {
            module.onConnect(ctx);
            module.start();

            CommandResult<GdResyncResult> result =
                    module.handleGdResync(GdResyncCommand.builder().build(), null);

            assertTrue(result.isOk());
        }
    }

    @Nested
    class NoDoubleBurst {

        @Test
        void publishSnapshot_shouldDisarmTheFallbackPoll_afterExactlyOneBurst() {
            TestGameDataReadinessProvider.ready = false;
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));

            module.onConnect(ctx);
            module.start();
            assertTrue(module.readinessPollArmed(), "the fallback poll must be armed while the host is unready");
            assertTrue(recorded.isEmpty(), "deferred while unready");

            TestGameDataReadinessProvider.ready = true;
            // Drive the burst the way the host itself does — via its own registered
            // NxGameData.publishSnapshot() call — not via the fallback poll.
            gameData.publishSnapshot();

            List<GameDataSyncEvent<?>> itemEvents = eventsFor("itemtemplate");
            assertEquals(2, itemEvents.size(), "exactly one full burst — one UPSERT + one SNAPSHOT_COMPLETE");
            // The disarm is what actually stops the double burst: readinessPoll is a
            // ScheduledFuture handle, and cancel()-ing it (done inside runAllSnapshots, before
            // publishing) guarantees the JDK scheduler never invokes that recurring task again —
            // so no further tick can ever fire. Re-invoking the package-visible
            // pollReadinessOnce() by hand afterwards is a distinct, independently-valid resync
            // trigger (same as a fresh GdResyncCommand), not "the same tick that got cancelled",
            // so it is expected to publish again rather than proving anything about the poll.
            assertFalse(module.readinessPollArmed(), "the host's own publish must disarm the fallback poll");
        }
    }

    @Nested
    class BackCompat {

        @Test
        void start_shouldPublishFullBurst_whenReadinessListIsEmpty() {
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));
            GameDataSyncModule noReadinessModule = new GameDataSyncModule(
                    GameDataSyncModule.defaultDescriptors(),
                    sender,
                    GameDataSyncConfig.defaults(),
                    Collections.<GameDataReadinessProvider>emptyList());

            noReadinessModule.onConnect(ctx);
            try {
                noReadinessModule.start();

                List<GameDataSyncEvent<?>> itemEvents = eventsFor("itemtemplate");
                assertEquals(
                        2,
                        itemEvents.size(),
                        "an empty readiness-provider list behaves exactly as before the readiness feature");
                assertEquals(
                        GameDataSnapshotPublisher.OP_UPSERT, itemEvents.get(0).getOp());
                assertEquals(
                        GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE,
                        itemEvents.get(1).getOp());
            } finally {
                noReadinessModule.onDisconnect();
            }
        }
    }

    @Nested
    class DuplicateProviders {

        @Test
        void onConnect_shouldFail_whenTwoReadinessProvidersRegistered() {
            List<GameDataReadinessProvider> duplicates = new ArrayList<GameDataReadinessProvider>();
            duplicates.add(() -> true);
            duplicates.add(() -> true);
            GameDataSyncModule duplicateModule = new GameDataSyncModule(
                    GameDataSyncModule.defaultDescriptors(), sender, GameDataSyncConfig.defaults(), duplicates);

            try {
                duplicateModule.onConnect(ctx);
                duplicateModule.start();

                assertEquals(
                        GameDataSyncModule.STATE_FAILED,
                        duplicateModule.currentStatus().getState());
                assertTrue(recorded.isEmpty(), "a FAILED module must publish nothing");
            } finally {
                duplicateModule.onDisconnect();
            }
        }
    }

    @Nested
    class ReArm {

        @Test
        void publishSnapshot_shouldReArmThePoll_whenHostGoesUnreadyAgain() {
            TestItemTemplateProvider.snapshot = Collections.singletonList(itemTemplate(1));
            // TestGameDataReadinessProvider.ready defaults to true.

            module.onConnect(ctx);
            module.start();
            assertFalse(recorded.isEmpty(), "initial burst while the host starts ready");
            assertFalse(module.readinessPollArmed(), "no poll needed while the host stays ready");

            recorded.clear();
            TestGameDataReadinessProvider.ready = false;
            // Host-driven pass (e.g. a datapack reload notification) while unready again.
            gameData.publishSnapshot();

            assertTrue(recorded.isEmpty(), "nothing published while unready again");
            assertTrue(module.readinessPollArmed(), "the poll must re-arm once the host goes unready again");
        }
    }

    private static final class RecordingSender implements GameDataSender {

        private final List<GameDataSyncEvent<?>> sink;

        RecordingSender(List<GameDataSyncEvent<?>> sink) {
            this.sink = sink;
        }

        @Override
        public void send(ProducerRecord<byte[], Object> record, Callback callback) {
            sink.add((GameDataSyncEvent<?>) record.value());
            callback.onCompletion(null, null);
        }
    }

    /**
     * Captures the trigger the module registers via {@link NxGameData#registerSnapshotTrigger} so
     * tests can drive a snapshot the same way a host does — by calling {@link #publishSnapshot()} —
     * rather than reaching into the module's private scheduling internals.
     */
    private static final class CapturingGameData implements NxGameData {

        private volatile NxGameDataTrigger trigger;

        @Override
        public void publishSnapshot() {
            NxGameDataTrigger t = trigger;
            if (t != null) {
                t.run();
            }
        }

        @Override
        public void registerSnapshotTrigger(NxGameDataTrigger trigger) {
            this.trigger = trigger;
        }
    }
}
