package app.l2nx.gs.gd.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.kafka.ops.model.EntityStats;
import app.l2nx.gs.adapter.api.kafka.sync.gd.GameDataSyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.gd.experiencelevel.ExperienceLevel;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExperienceLevelDescriptorTest {

    private static final String TOPIC = "kbt.gd.sync.experiencelevel";

    private List<GameDataSyncEvent<?>> recorded;
    private GameDataSyncModule module;

    @BeforeEach
    void setUp() {
        recorded = new ArrayList<GameDataSyncEvent<?>>();
        GameDataSender sender = new GameDataSender() {
            @Override
            public void send(ProducerRecord<byte[], Object> record, Callback callback) {
                recorded.add((GameDataSyncEvent<?>) record.value());
                callback.onCompletion(null, null);
            }
        };
        module = new GameDataSyncModule(GameDataSyncModule.defaultDescriptors(), sender);
    }

    @AfterEach
    void tearDown() {
        TestExperienceLevelProvider.snapshot = Collections.emptyList();
    }

    private void connectAndSnapshot() {
        SyncTopics topics = SyncTopics.builder()
                .gd(Collections.singletonMap("experiencelevel", TOPIC))
                .build();
        ConnectContext ctx = ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("kbt")
                .serverId(UUID.randomUUID())
                .serverSlug("kbt-x1")
                .serverName("x1")
                .adapterVersion("test")
                .syncTopics(topics)
                .build();
        module.onConnect(ctx);
        module.start();
    }

    private List<GameDataSyncEvent<?>> experienceEvents() {
        List<GameDataSyncEvent<?>> out = new ArrayList<GameDataSyncEvent<?>>();
        for (GameDataSyncEvent<?> e : recorded) {
            if ("experiencelevel".equals(e.getEntityName())) {
                out.add(e);
            }
        }
        return out;
    }

    @Nested
    class Resolve {

        @Test
        void onConnect_shouldIncludeExperienceLevel_inActiveEntities() {
            TestExperienceLevelProvider.snapshot = Collections.singletonList(
                    ExperienceLevel.builder().level(1).requiredExp(0L).build());

            connectAndSnapshot();

            List<EntityStats> entities =
                    module.currentStatus().getStats().getEntities().orElseGet(Collections::emptyList);
            List<String> names = new ArrayList<String>();
            for (EntityStats s : entities) {
                names.add(s.getName());
            }
            assertTrue(names.contains("experiencelevel"));
        }
    }

    @Nested
    class PublishSnapshot {

        @Test
        void start_shouldPublishRowPerLevelKeyedByLevel_thenSnapshotComplete() {
            ExperienceLevel l1 =
                    ExperienceLevel.builder().level(1).requiredExp(0L).build();
            ExperienceLevel l2 =
                    ExperienceLevel.builder().level(2).requiredExp(68L).build();
            TestExperienceLevelProvider.snapshot = Arrays.asList(l1, l2);

            connectAndSnapshot();

            List<GameDataSyncEvent<?>> events = experienceEvents();
            assertEquals(3, events.size(), "2 UPSERT + 1 SNAPSHOT_COMPLETE");
            assertEquals(GameDataSnapshotPublisher.OP_UPSERT, events.get(0).getOp());
            assertEquals(Long.valueOf(1L), events.get(0).getPk());
            assertEquals(l1, events.get(0).getPayload());
            assertEquals(Long.valueOf(2L), events.get(1).getPk());
            assertEquals(l2, events.get(1).getPayload());
            GameDataSyncEvent<?> complete = events.get(2);
            assertEquals(GameDataSnapshotPublisher.OP_SNAPSHOT_COMPLETE, complete.getOp());
            assertEquals(Integer.valueOf(2), complete.getCount());
            assertEquals(events.get(0).getSyncId(), complete.getSyncId(), "burst shares one syncId");
        }
    }
}
