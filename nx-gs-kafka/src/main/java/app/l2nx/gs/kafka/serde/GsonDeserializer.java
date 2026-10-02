package app.l2nx.gs.kafka.serde;

import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/** Public utility; the internal poll loop deserializes from raw bytes itself. */
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
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
    }

    @Override
    public void close() {}
}
