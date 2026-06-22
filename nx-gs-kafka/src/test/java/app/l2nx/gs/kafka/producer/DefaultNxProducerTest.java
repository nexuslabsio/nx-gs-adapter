package app.l2nx.gs.kafka.producer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class DefaultNxProducerTest {

    private static final String TOPIC = "test.topic";
    private static final String HEADER_NAME = "X-Static";
    private static final byte[] HEADER_VALUE = new byte[] {1, 2, 3};

    @Test
    void stamp_shouldEmitFreshHeaderInstance_perRecord() {
        MockProducer<byte[], Object> mock =
                new MockProducer<>(true, new ByteArraySerializer(), (topic, data) -> new byte[0]);
        Map<String, byte[]> headers = new HashMap<>();
        headers.put(HEADER_NAME, HEADER_VALUE);
        DefaultNxProducer producer = new DefaultNxProducer(mock, headers, Duration.ofSeconds(1));

        producer.send(TOPIC, "k1", "v1");
        producer.send(TOPIC, "k2", "v2");

        List<ProducerRecord<byte[], Object>> sent = mock.history();
        assertEquals(2, sent.size());

        Header h0 = sent.get(0).headers().lastHeader(HEADER_NAME);
        Header h1 = sent.get(1).headers().lastHeader(HEADER_NAME);

        assertNotSame(h0, h1, "Each record must get its own RecordHeader instance");
        assertEquals(HEADER_NAME, h0.key());
        assertEquals(HEADER_NAME, h1.key());
        assertArrayEquals(HEADER_VALUE, h0.value());
        assertArrayEquals(HEADER_VALUE, h1.value());
    }

    @Test
    void send_shouldEncodeStringKeyAsUtf8() {
        MockProducer<byte[], Object> mock =
                new MockProducer<>(true, new ByteArraySerializer(), (topic, data) -> new byte[0]);
        DefaultNxProducer producer =
                new DefaultNxProducer(mock, java.util.Collections.emptyMap(), Duration.ofSeconds(1));

        producer.send(TOPIC, "abc", "v");

        ProducerRecord<byte[], Object> record = mock.history().get(0);
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), record.key());
    }

    @Test
    void send_shouldUseNullKey_whenStringKeyNull() {
        MockProducer<byte[], Object> mock =
                new MockProducer<>(true, new ByteArraySerializer(), (topic, data) -> new byte[0]);
        DefaultNxProducer producer =
                new DefaultNxProducer(mock, java.util.Collections.emptyMap(), Duration.ofSeconds(1));

        producer.send(TOPIC, null, "v");

        ProducerRecord<byte[], Object> record = mock.history().get(0);
        assertNull(record.key());
    }
}
