package app.l2nx.gs.db.sync.engine.publish;

import app.l2nx.gs.adapter.api.kafka.sync.db.SyncEvent;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDbDto;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.db.sync.engine.TestMappings;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class SyncEventPublisherTest {

    private static final String TOPIC = "bohpts.gs.sync.clans";

    @Test
    void publish_shouldEmitSyncEvent_forCreated() throws Exception {
        RecordingSender sender = new RecordingSender();
        SyncEventPublisher publisher = new SyncEventPublisher(sender);

        ClanDbDto dto = ClanDbDto.builder().id(42L).name("Hellbound").level(5).build();
        CompletableFuture<RecordMetadata> future =
                publisher.publish(clanMapping(), SyncEventPublisher.OP_CREATED, 42L, dto, TOPIC);
        sender.completeLast(metadata());

        assertEquals(TOPIC, sender.lastTopic);
        assertArrayEquals(ByteBuffer.allocate(8).putLong(42L).array(), sender.lastKey);
        assertNotNull(sender.lastValue);
        assertInstanceOf(SyncEvent.class, sender.lastValue);
        SyncEvent<?> event = (SyncEvent<?>) sender.lastValue;
        assertEquals("clan", event.getEntityName());
        assertEquals(42L, event.getPk());
        assertEquals("CREATED", event.getOp());
        assertSame(dto, event.getPayload());
        assertTrue(event.getTimestampEpochMs() > 0L);
        assertNotNull(future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void publish_shouldEmitEnvelopeWithNullPayload_forDeleted() throws Exception {
        RecordingSender sender = new RecordingSender();
        SyncEventPublisher publisher = new SyncEventPublisher(sender);

        CompletableFuture<RecordMetadata> future =
                publisher.publish(clanMapping(), SyncEventPublisher.OP_DELETED, 42L, null, TOPIC);
        sender.completeLast(metadata());

        assertEquals(TOPIC, sender.lastTopic);
        assertArrayEquals(ByteBuffer.allocate(8).putLong(42L).array(), sender.lastKey);
        assertNotNull(sender.lastValue, "DELETED publishes a non-null SyncEvent envelope");
        assertInstanceOf(SyncEvent.class, sender.lastValue);
        SyncEvent<?> event = (SyncEvent<?>) sender.lastValue;
        assertEquals("clan", event.getEntityName());
        assertEquals(42L, event.getPk());
        assertEquals("DELETED", event.getOp());
        assertNull(event.getPayload(), "payload slot is null on DELETE — entity is gone");
        assertTrue(event.getTimestampEpochMs() > 0L);
        assertNotNull(future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void encodeKey_shouldMatch_kafkaLongSerializer() {
        // Cross-check: identical bytes for any long → engine and external
        // LongSerializer-based writers land on the same partition.
        try (LongSerializer ls = new LongSerializer()) {
            for (long pk : new long[]{0L, 1L, -1L, 12345L, Long.MIN_VALUE, Long.MAX_VALUE}) {
                assertArrayEquals(ls.serialize("topic", pk),
                        SyncEventPublisher.encodeKey(pk),
                        "LongSerializer parity for pk=" + pk);
            }
        }
    }

    @Test
    void publish_shouldFailFuture_whenSenderCallbackReportsException() {
        RecordingSender sender = new RecordingSender();
        SyncEventPublisher publisher = new SyncEventPublisher(sender);

        ClanDbDto dto = ClanDbDto.builder().id(1L).name("X").level(1).build();
        CompletableFuture<RecordMetadata> future =
                publisher.publish(clanMapping(), SyncEventPublisher.OP_UPDATED, 1L, dto, TOPIC);
        sender.completeLast(new RuntimeException("kafka down"));

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        assertEquals("kafka down", ex.getCause().getMessage());
    }

    @Test
    void publish_shouldFailFuture_whenSenderThrowsSynchronously() {
        KafkaSender sender = (topic, key, value, callback) -> {
            throw new IllegalStateException("send rejected");
        };
        SyncEventPublisher publisher = new SyncEventPublisher(sender);

        ClanDbDto dto = ClanDbDto.builder().id(1L).name("X").level(1).build();
        CompletableFuture<RecordMetadata> future =
                publisher.publish(clanMapping(), SyncEventPublisher.OP_CREATED, 1L, dto, TOPIC);

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        assertEquals("send rejected", ex.getCause().getMessage());
    }

    @Test
    void publish_shouldNotResolveFuture_untilSenderInvokesCallback() {
        RecordingSender sender = new RecordingSender();
        SyncEventPublisher publisher = new SyncEventPublisher(sender);

        ClanDbDto dto = ClanDbDto.builder().id(1L).name("X").level(1).build();
        CompletableFuture<RecordMetadata> future =
                publisher.publish(clanMapping(), SyncEventPublisher.OP_CREATED, 1L, dto, TOPIC);

        assertThrows(TimeoutException.class, () -> future.get(50, TimeUnit.MILLISECONDS));
    }

    private static RecordMetadata metadata() {
        return new RecordMetadata(new TopicPartition(TOPIC, 0), 0L, 0, 0L, 0, 0);
    }

    private static EntityMapping<ClanDbDto> clanMapping() {
        return TestMappings.clanOnly();
    }

    private static final class RecordingSender implements KafkaSender {
        String lastTopic;
        byte[] lastKey;
        Object lastValue;
        final List<Callback> callbacks = new ArrayList<Callback>();

        @Override
        public void send(String topic, byte[] key, Object value, Callback callback) {
            this.lastTopic = topic;
            this.lastKey = key;
            this.lastValue = value;
            this.callbacks.add(callback);
        }

        void completeLast(RecordMetadata metadata) {
            callbacks.get(callbacks.size() - 1).onCompletion(metadata, null);
        }

        void completeLast(Exception exception) {
            callbacks.get(callbacks.size() - 1).onCompletion(null, exception);
        }
    }
}
