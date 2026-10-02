package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import com.google.gson.Gson;
import java.util.Map;
import java.util.function.BiConsumer;

/** Internal; use {@link app.l2nx.gs.kafka.NxKafka#subscribe}. */
public interface NxConsumer {

    void stop();

    static <T> NxConsumer create(
            String topic,
            Class<T> type,
            BiConsumer<T, ReplyContext> handler,
            NxProducer producer,
            Gson gson,
            Map<String, Object> consumerConfig) {
        return new ConsumerGroup<>(topic, type, handler, producer, gson, consumerConfig);
    }
}
