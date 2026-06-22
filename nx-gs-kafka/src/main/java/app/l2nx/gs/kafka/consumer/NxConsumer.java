package app.l2nx.gs.kafka.consumer;

import app.l2nx.gs.kafka.producer.NxProducer;
import com.google.gson.Gson;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Internal consumer interface. Users should call
 * {@link app.l2nx.gs.kafka.NxKafka#subscribe} methods instead.
 */
public interface NxConsumer {

    /**
     * Stops the poll loop thread and closes the underlying Kafka consumer.
     */
    void stop();

    /**
     * Creates a new consumer group for the given topic with a dedicated poll-loop thread.
     *
     * @param topic          Kafka topic name
     * @param type           message class for Gson deserialization
     * @param handler        invoked for each message with a {@link ReplyContext}
     * @param producer       producer used by ReplyContext to send replies
     * @param consumerConfig Kafka consumer configuration properties
     * @param <T>            message type
     * @return a new consumer instance
     */
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
