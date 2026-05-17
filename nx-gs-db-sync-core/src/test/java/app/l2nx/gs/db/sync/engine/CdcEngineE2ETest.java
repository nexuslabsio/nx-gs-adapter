package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.*;
import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDto;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanSkillDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
import app.l2nx.gs.db.sync.engine.persist.NoopSnapshotPersistence;
import app.l2nx.gs.db.sync.engine.phase.Phase1Hasher;
import app.l2nx.gs.db.sync.engine.phase.Phase2Fetcher;
import app.l2nx.gs.db.sync.engine.publish.KafkaSender;
import app.l2nx.gs.db.sync.engine.publish.SyncEventPublisher;
import app.l2nx.gs.db.sync.engine.publish.TopicResolver;
import app.l2nx.gs.db.sync.engine.window.WindowPlanner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class CdcEngineE2ETest {

    private static final String TOPIC = "test.gs.sync.clans";
    private static final Gson GSON = new Gson();

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("bohpts")
            .withUsername("test")
            .withPassword("test");

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            "confluentinc/cp-kafka:7.7.0");

    private static KafkaProducer<byte[], byte[]> producer;
    private CdcEngine engine;

    @BeforeAll
    static void setupSchemaAndProducer() throws SQLException {
        try (Connection c = jdbc(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE clan_data (" +
                    "  clan_id BIGINT PRIMARY KEY," +
                    "  clan_name VARCHAR(64) NOT NULL," +
                    "  clan_level INT NOT NULL DEFAULT 0," +
                    "  leader_id BIGINT NOT NULL DEFAULT 0," +
                    "  ally_id BIGINT NOT NULL DEFAULT 0)");
            st.execute("CREATE TABLE clan_skills (" +
                    "  clan_id INT NOT NULL DEFAULT 0," +
                    "  skill_id INT NOT NULL DEFAULT 0," +
                    "  skill_level INT NOT NULL DEFAULT 0," +
                    "  skill_name VARCHAR(26) NULL," +
                    "  sub_pledge_id INT NOT NULL DEFAULT -2," +
                    "  PRIMARY KEY (clan_id, skill_id, sub_pledge_id))");
            st.execute("INSERT INTO clan_data VALUES (1, 'Hellbound', 5, 100, 0)");
            st.execute("INSERT INTO clan_data VALUES (2, 'Phoenix', 3, 200, 50)");
            st.execute("INSERT INTO clan_data VALUES (3, 'Dragons', 7, 300, 50)");
            // Clan 1 — 2 skills; clan 2 — 2 skills; clan 3 — none.
            st.execute("INSERT INTO clan_skills (clan_id, skill_id, skill_level) VALUES (1, 101, 1)");
            st.execute("INSERT INTO clan_skills (clan_id, skill_id, skill_level) VALUES (1, 102, 2)");
            st.execute("INSERT INTO clan_skills (clan_id, skill_id, skill_level) VALUES (2, 201, 5)");
            st.execute("INSERT INTO clan_skills (clan_id, skill_id, skill_level) VALUES (2, 202, 3)");
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producer = new KafkaProducer<>(props);
    }

    @AfterAll
    static void teardownProducer() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(2));
        }
    }

    @AfterEach
    void stopEngine() {
        if (engine != null) {
            engine.stop();
            engine = null;
        }
    }

    @Test
    void engine_shouldSyncMultiSourceEntity_includingChildCrudAndEnvelopeBoundary() throws Exception {
        EntityStatsTracker statsTracker = new EntityStatsTracker();
        engine = buildEngine(statsTracker);
        engine.start();

        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer()) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // Cycle 1 — initial sync: 3 CREATED events; payloads carry the assembled
            // skills lists pulled from clan_skills.
            awaitTick();
            List<ConsumerRecord<byte[], byte[]>> initial = poll(consumer, 3);
            assertEquals(3, initial.size(), "initial sync emits one event per existing clan");
            Map<Long, ClanDto> byClanId = new HashMap<>();
            for (ConsumerRecord<byte[], byte[]> record : initial) {
                SyncEvent<ClanDto> event = decode(record.value());
                assertEquals("clan", event.getEntityName());
                assertEquals("CREATED", event.getOp());
                assertEquals(decodeKey(record.key()), event.getPk());
                assertNotNull(event.getPayload());
                byClanId.put(event.getPk(), event.getPayload());
            }
            assertEquals(2, byClanId.get(1L).getSkills().size(), "clan 1 starts with 2 skills");
            assertEquals(2, byClanId.get(2L).getSkills().size(), "clan 2 starts with 2 skills");
            assertEquals(0, byClanId.get(3L).getSkills().size(), "clan 3 has no skills");
            assertSkillsContain(byClanId.get(1L), 101, 1);
            assertSkillsContain(byClanId.get(1L), 102, 2);

            // Cycle 2 — primary-only mutation: rename clan 2 → exactly one UPDATED.
            try (Connection c = jdbc(); PreparedStatement ps = c.prepareStatement(
                    "UPDATE clan_data SET clan_name = ? WHERE clan_id = ?")) {
                ps.setString(1, "Phoenix-renamed");
                ps.setLong(2, 2L);
                ps.executeUpdate();
            }
            awaitTick();
            SyncEvent<ClanDto> renameEvent = expectSingleEvent(consumer, "UPDATED");
            assertEquals(2L, renameEvent.getPk());
            assertEquals("Phoenix-renamed", renameEvent.getPayload().getClanName());
            assertEquals(2, renameEvent.getPayload().getSkills().size(),
                    "skill list survives a primary-only rename");

            // Cycle 3 — child INSERT: add a new skill row for clan 1 → UPDATED with 3 skills.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO clan_skills (clan_id, skill_id, skill_level) VALUES (1, 103, 4)");
            }
            awaitTick();
            SyncEvent<ClanDto> addSkillEvent = expectSingleEvent(consumer, "UPDATED");
            assertEquals(1L, addSkillEvent.getPk());
            assertEquals(3, addSkillEvent.getPayload().getSkills().size());
            assertSkillsContain(addSkillEvent.getPayload(), 103, 4);

            // Cycle 4 — child UPDATE: change a skill level for clan 2 → UPDATED with new level.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("UPDATE clan_skills SET skill_level = 99 "
                        + "WHERE clan_id = 2 AND skill_id = 201");
            }
            awaitTick();
            SyncEvent<ClanDto> levelEvent = expectSingleEvent(consumer, "UPDATED");
            assertEquals(2L, levelEvent.getPk());
            assertSkillsContain(levelEvent.getPayload(), 201, 99);

            // Cycle 5 — child DELETE: drop a skill for clan 1 → UPDATED with 2 skills.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM clan_skills WHERE clan_id = 1 AND skill_id = 101");
            }
            awaitTick();
            SyncEvent<ClanDto> dropSkillEvent = expectSingleEvent(consumer, "UPDATED");
            assertEquals(1L, dropSkillEvent.getPk());
            assertEquals(2, dropSkillEvent.getPayload().getSkills().size());

            // Cycle 6 — primary DELETE on a non-boundary row (clan 2) → tombstone.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM clan_data WHERE clan_id = 2");
            }
            awaitTick();
            ConsumerRecord<byte[], byte[]> tombstone1 = expectSingleTombstone(consumer);
            assertEquals(2L, decodeKey(tombstone1.key()));

            // Cycle 7 — envelope regression: delete the current MAX(clan_id) row (clan 3).
            // Pre-fix, the next cycle's MAX_db would shrink to 1 and clan 3 would
            // never re-enter any window — its DELETE would silently never fire.
            // Post-fix, the window envelope includes max(MAX_db, MAX_snapshot) = 3,
            // so the window covers PK 3 and the diff produces a tombstone.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM clan_data WHERE clan_id = 3");
            }
            awaitTick();
            ConsumerRecord<byte[], byte[]> tombstone2 = expectSingleTombstone(consumer);
            assertEquals(3L, decodeKey(tombstone2.key()),
                    "envelope-based windowing must catch deletion of the prior MAX(pk)");
        }

        // Stats — entity HEALTHY, consecutiveErrors stays at 0 across the run.
        List<EntityStats> entities = statsTracker.currentStatuses();
        assertEquals(1, entities.size());
        EntityStats clanStats = entities.get(0);
        assertEquals("clan", clanStats.getName());
        assertEquals(EntityState.HEALTHY, clanStats.getState());
        assertEquals(Integer.valueOf(0), clanStats.getConsecutiveErrors());

        // HeartbeatEvent shape — db-sync module ACTIVE, entities[clan]=HEALTHY.
        ModuleStatus dbSyncStatus = ModuleStatus.builder()
                .name("db-sync")
                .state("ACTIVE")
                .stats(ModuleStatus.Stats.builder().entities(entities).build())
                .build();
        HeartbeatEvent heartbeat = HeartbeatEvent.builder()
                .tenantId("test-tenant")
                .serverId("test-server")
                .adapterVersion("0.1.0")
                .uptimeMs(1000L)
                .enabledModules(Collections.singletonList(dbSyncStatus))
                .build();
        assertEquals(1, heartbeat.getEnabledModules().size());
        ModuleStatus heartbeatModule = heartbeat.getEnabledModules().get(0);
        assertEquals("db-sync", heartbeatModule.getName());
        assertEquals("ACTIVE", heartbeatModule.getState());
        assertTrue(heartbeatModule.getStats().getEntities().isPresent());
        List<EntityStats> heartbeatEntities = heartbeatModule.getStats().getEntities().get();
        assertEquals(1, heartbeatEntities.size());
        assertEquals("clan", heartbeatEntities.get(0).getName());
        assertEquals(EntityState.HEALTHY, heartbeatEntities.get(0).getState());
    }

    private static void assertSkillsContain(ClanDto dto, int skillId, int skillLevel) {
        for (ClanSkillDto s : dto.getSkills()) {
            if (s.getSkillId() == skillId && s.getSkillLevel() == skillLevel) {
                return;
            }
        }
        fail("expected skill " + skillId + ":" + skillLevel + " in " + dto.getSkills());
    }

    private SyncEvent<ClanDto> expectSingleEvent(KafkaConsumer<byte[], byte[]> consumer, String op) {
        List<ConsumerRecord<byte[], byte[]>> records = poll(consumer, 1);
        assertEquals(1, records.size(), "expected exactly one " + op + " event");
        SyncEvent<ClanDto> event = decode(records.get(0).value());
        assertEquals(op, event.getOp());
        return event;
    }

    private ConsumerRecord<byte[], byte[]> expectSingleTombstone(KafkaConsumer<byte[], byte[]> consumer) {
        List<ConsumerRecord<byte[], byte[]>> records = poll(consumer, 1);
        assertEquals(1, records.size(), "expected exactly one DELETE event");
        ConsumerRecord<byte[], byte[]> record = records.get(0);
        assertNotNull(record.value(), "DELETE wire shape is a SyncEvent envelope, not a Kafka tombstone");
        SyncEvent<ClanDto> event = decode(record.value());
        assertEquals("DELETED", event.getOp());
        assertNull(event.getPayload(), "payload slot is null on DELETE");
        assertTrue(event.getTimestampEpochMs() > 0L);
        return record;
    }

    private CdcEngine buildEngine(EntityStatsTracker statsTracker) {
        EngineConfig config = new EngineConfig(
                /* tickIntervalSeconds */ 60, // unused — driven by tickOnceSynchronously()
                /* rowsPerWindow */ 500_000,
                /* queryTimeoutSeconds */ 10,
                /* publishFlushSeconds */ 5);
        TopicResolver topicResolver = entityName -> "clan".equals(entityName) ? TOPIC : null;
        SyncEventPublisher publisher = new SyncEventPublisher(testKafkaSender());
        List<EntityMapping<?>> mappings = Collections.singletonList(TestMappings.clanWithSkills());
        return new CdcEngine(
                "test",
                mappings,
                new TestcontainerJdbcSource(),
                new SnapshotStore(),
                NoopSnapshotPersistence.INSTANCE,
                config,
                topicResolver,
                publisher,
                statsTracker,
                new WindowPlanner(),
                new Phase1Hasher(),
                new Phase2Fetcher(),
                key -> null);
    }

    private KafkaSender testKafkaSender() {
        return (topic, key, value, callback) -> {
            byte[] valueBytes = value == null ? null
                    : GSON.toJson(value).getBytes(StandardCharsets.UTF_8);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, valueBytes);
            producer.send(record, callback);
        };
    }

    private void awaitTick() throws InterruptedException, ExecutionException, TimeoutException {
        List<Future<?>> ticks = engine.tickOnceSynchronously();
        for (Future<?> f : ticks) {
            f.get(30, TimeUnit.SECONDS);
        }
        producer.flush();
    }

    private KafkaConsumer<byte[], byte[]> newConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-e2e-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static List<ConsumerRecord<byte[], byte[]>> poll(KafkaConsumer<byte[], byte[]> consumer,
                                                             int expected) {
        // Single poll loop — polling itself drives metadata refresh and group
        // join, so a record-collecting loop covers both the assignment-wait and
        // the record-delivery phases. A separate "drain until assigned" wait
        // would silently consume records and discard them.
        List<ConsumerRecord<byte[], byte[]>> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline && collected.size() < expected) {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<byte[], byte[]> record : batch) {
                collected.add(record);
            }
        }
        return collected;
    }

    private static final java.lang.reflect.Type SYNC_EVENT_TYPE =
            new TypeToken<SyncEvent<ClanDto>>() {
            }.getType();

    private static SyncEvent<ClanDto> decode(byte[] valueBytes) {
        assertNotNull(valueBytes, "non-tombstone events must have a payload");
        return GSON.fromJson(new String(valueBytes, StandardCharsets.UTF_8), SYNC_EVENT_TYPE);
    }

    private static long decodeKey(byte[] keyBytes) {
        assertEquals(Long.BYTES, keyBytes.length, "key is 8-byte big-endian long");
        return ByteBuffer.wrap(keyBytes).getLong();
    }

    private static Connection jdbc() throws SQLException {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
    }

    private static final class TestcontainerJdbcSource implements JdbcConnectionSource {
        @Override
        public String name() {
            return "testcontainer-mysql";
        }

        @Override
        public Connection getConnection() throws SQLException {
            return jdbc();
        }

        @Override
        public Optional<PoolStats> stats() {
            return Optional.empty();
        }
    }
}
