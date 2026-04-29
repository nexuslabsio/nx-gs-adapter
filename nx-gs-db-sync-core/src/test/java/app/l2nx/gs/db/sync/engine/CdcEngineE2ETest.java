package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.ops.EntityState;
import app.l2nx.gs.adapter.api.kafka.ops.EntityStats;
import app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent;
import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.kafka.ops.PoolStats;
import app.l2nx.gs.adapter.api.kafka.sync.db.ClanDto;
import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.JdbcConnectionSource;
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
            st.execute("INSERT INTO clan_data VALUES (1, 'Hellbound', 5, 100, 0)");
            st.execute("INSERT INTO clan_data VALUES (2, 'Phoenix', 3, 200, 50)");
            st.execute("INSERT INTO clan_data VALUES (3, 'Dragons', 7, 300, 50)");
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
    void engine_shouldEmitCreatedUpdatedDeleted_overThreeCycles() throws Exception {
        EntityStatsTracker statsTracker = new EntityStatsTracker();
        engine = buildEngine(statsTracker);
        engine.start();

        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer()) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // First tick — initial sync emits 3 CREATED events.
            awaitTick();
            List<ConsumerRecord<byte[], byte[]>> firstCycle = poll(consumer, 3);
            assertEquals(3, firstCycle.size(), "initial sync emits one event per existing row");
            for (ConsumerRecord<byte[], byte[]> record : firstCycle) {
                SyncEvent<ClanDto> event = decode(record.value());
                assertEquals("clan", event.getEntityName());
                assertEquals("CREATED", event.getOp());
                assertEquals(decodeKey(record.key()), event.getPk());
                assertNotNull(event.getPayload());
                assertEquals(event.getPk(), event.getPayload().getClanId());
            }

            // UPDATE one row — next cycle emits 1 UPDATED.
            try (Connection c = jdbc(); PreparedStatement ps = c.prepareStatement(
                    "UPDATE clan_data SET clan_name = ? WHERE clan_id = ?")) {
                ps.setString(1, "Phoenix-renamed");
                ps.setLong(2, 2L);
                ps.executeUpdate();
            }
            awaitTick();
            List<ConsumerRecord<byte[], byte[]>> updateCycle = poll(consumer, 1);
            assertEquals(1, updateCycle.size(), "single row update emits a single UPDATED event");
            ConsumerRecord<byte[], byte[]> updateRecord = updateCycle.get(0);
            SyncEvent<ClanDto> updateEvent = decode(updateRecord.value());
            assertEquals("UPDATED", updateEvent.getOp());
            assertEquals(2L, updateEvent.getPk());
            assertNotNull(updateEvent.getPayload());
            assertEquals("Phoenix-renamed", updateEvent.getPayload().getClanName());

            // DELETE a non-boundary row — next cycle emits 1 DELETED tombstone.
            // Boundary rows (current MIN/MAX) are deliberately preserved so the
            // window [MIN(pk), MAX(pk)] does not shrink mid-test; shrinking-window
            // semantics are out of scope for this e2e and exercised separately.
            try (Connection c = jdbc(); Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM clan_data WHERE clan_id = 2");
            }
            awaitTick();
            List<ConsumerRecord<byte[], byte[]>> deleteCycle = poll(consumer, 1);
            assertEquals(1, deleteCycle.size(), "single row delete emits a single tombstone");
            ConsumerRecord<byte[], byte[]> deleteRecord = deleteCycle.get(0);
            assertEquals(2L, decodeKey(deleteRecord.key()));
            assertNull(deleteRecord.value(), "tombstone payload is null on the wire");
        }

        // Stats — entity HEALTHY, consecutiveErrors stays at 0 across the run.
        List<EntityStats> entities = statsTracker.currentStatuses();
        assertEquals(1, entities.size());
        EntityStats clanStats = entities.get(0);
        assertEquals("clan", clanStats.getName());
        assertEquals(EntityState.HEALTHY, clanStats.getState());
        assertEquals(Integer.valueOf(0), clanStats.getConsecutiveErrors());

        // HeartbeatEvent shape — db-sync module ACTIVE, entities[clan]=HEALTHY. The
        // engine's tracker output composes cleanly into a wire heartbeat. DbSyncModule
        // wraps the tracker the same way; that wrap is unit-tested separately.
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

    private CdcEngine buildEngine(EntityStatsTracker statsTracker) {
        EngineConfig config = new EngineConfig(
                /* tickIntervalSeconds */ 60, // unused — driven by tickOnceSynchronously()
                /* rowsPerWindow */ 500_000,
                /* queryTimeoutSeconds */ 10,
                /* publishFlushSeconds */ 5);
        TopicResolver topicResolver = entityName -> "clan".equals(entityName) ? TOPIC : null;
        SyncEventPublisher publisher = new SyncEventPublisher(testKafkaSender());
        List<EntityMapping<?>> mappings = Collections.singletonList(new TestClanMapping());
        return new CdcEngine(
                "test",
                mappings,
                new TestcontainerJdbcSource(),
                new SnapshotStore(),
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

    private static final class TestClanMapping implements EntityMapping<ClanDto> {
        private static final List<String> HASHED = Collections.unmodifiableList(
                Arrays.asList("clan_name", "clan_level", "leader_id", "ally_id"));

        @Override
        public String entityName() {
            return "clan";
        }

        @Override
        public String tableName() {
            return "clan_data";
        }

        @Override
        public String pkColumn() {
            return "clan_id";
        }

        @Override
        public List<String> hashedColumns() {
            return HASHED;
        }

        @Override
        public Class<ClanDto> dtoType() {
            return ClanDto.class;
        }

        @Override
        public ClanDto mapRow(ResultSet rs) throws SQLException {
            return ClanDto.builder()
                    .clanId(rs.getLong("clan_id"))
                    .clanName(rs.getString("clan_name"))
                    .clanLevel(rs.getInt("clan_level"))
                    .leaderId(nullIfZero(rs.getLong("leader_id")))
                    .allyId(nullIfZero(rs.getLong("ally_id")))
                    .build();
        }

        private static Long nullIfZero(long v) {
            return v == 0L ? null : v;
        }
    }
}
