package app.l2nx.gs.kafka.serde;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class GsonDeserializerTest {

    private final GsonDeserializer<TestEvent> deserializer = new GsonDeserializer<>(TestEvent.class);

    @Test
    void deserialize_shouldReturnTypedObject() {
        byte[] json = "{\"name\":\"player1\",\"value\":100}".getBytes(StandardCharsets.UTF_8);

        TestEvent result = deserializer.deserialize("test-topic", json);

        assertEquals("player1", result.name);
        assertEquals(100, result.value);
    }

    @Test
    void deserialize_shouldReturnNull_whenDataIsNull() {
        TestEvent result = deserializer.deserialize("test-topic", null);

        assertNull(result);
    }

    @Test
    void deserialize_shouldHandleNestedObjects() {
        byte[] json = "{\"label\":\"wrap\",\"nested\":{\"name\":\"p\",\"value\":1}}"
                .getBytes(StandardCharsets.UTF_8);
        GsonDeserializer<Wrapper> wrapperDeserializer = new GsonDeserializer<>(Wrapper.class);

        Wrapper result = wrapperDeserializer.deserialize("test-topic", json);

        assertEquals("wrap", result.label);
        assertNotNull(result.nested);
        assertEquals("p", result.nested.name);
    }

    @Test
    void deserialize_shouldIgnoreUnknownFields() {
        byte[] json = "{\"name\":\"p\",\"value\":1,\"extra\":\"ignored\"}"
                .getBytes(StandardCharsets.UTF_8);

        TestEvent result = deserializer.deserialize("test-topic", json);

        assertEquals("p", result.name);
        assertEquals(1, result.value);
    }

    static class TestEvent {
        String name;
        int value;
    }

    static class Wrapper {
        String label;
        TestEvent nested;
    }
}
