package app.l2nx.gs.kafka.serde;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class GsonSerializerTest {

    private final GsonSerializer serializer = new GsonSerializer();

    @Test
    void serialize_shouldReturnJsonBytes() {
        TestEvent event = new TestEvent("player1", 100);

        byte[] result = serializer.serialize("test-topic", event);

        String json = new String(result, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"name\":\"player1\""));
        assertTrue(json.contains("\"value\":100"));
    }

    @Test
    void serialize_shouldReturnNull_whenDataIsNull() {
        byte[] result = serializer.serialize("test-topic", null);

        assertNull(result);
    }

    @Test
    void serialize_shouldHandleString() {
        byte[] result = serializer.serialize("test-topic", "hello");

        assertEquals("\"hello\"", new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void serialize_shouldHandleNestedObjects() {
        TestEvent event = new TestEvent("player1", 100);
        Wrapper wrapper = new Wrapper("wrap", event);

        byte[] result = serializer.serialize("test-topic", wrapper);

        String json = new String(result, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"label\":\"wrap\""));
        assertTrue(json.contains("\"name\":\"player1\""));
    }

    @Test
    void serialize_shouldHandleEmptyObject() {
        byte[] result = serializer.serialize("test-topic", new Empty());

        assertEquals("{}", new String(result, StandardCharsets.UTF_8));
    }

    static class TestEvent {
        final String name;
        final int value;

        TestEvent(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    static class Wrapper {
        final String label;
        final TestEvent nested;

        Wrapper(String label, TestEvent nested) {
            this.label = label;
            this.nested = nested;
        }
    }

    static class Empty {
    }
}
