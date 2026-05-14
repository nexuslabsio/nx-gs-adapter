package app.l2nx.gs.kafka.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Kafka {@link Deserializer} that converts JSON bytes to a typed object via Gson.
 * This is a public utility for external users. Internally, nx-gs-kafka uses
 * {@code ByteArrayDeserializer} and deserializes in the poll loop.
 *
 * <pre>{@code
 * GsonDeserializer<MyEvent> deserializer = new GsonDeserializer<>(MyEvent.class);
 * }</pre>
 *
 * @param <T> the target type
 */
public class GsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson;
    private final Class<T> type;

    public GsonDeserializer(Class<T> type) {
        this(type, new Gson());
    }

    public GsonDeserializer(Class<T> type, Gson gson) {
        this.type = type;
        this.gson = gson;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
    }

    @Override
    public void close() {
    }
}
