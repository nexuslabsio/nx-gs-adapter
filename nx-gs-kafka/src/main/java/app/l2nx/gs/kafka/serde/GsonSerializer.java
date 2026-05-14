package app.l2nx.gs.kafka.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GsonSerializer implements Serializer<Object> {

    private final Gson gson;

    public GsonSerializer() {
        this(new Gson());
    }

    public GsonSerializer(Gson gson) {
        this.gson = gson;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
    }
}
