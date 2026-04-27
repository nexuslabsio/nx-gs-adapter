package app.l2nx.gs.kafka.producer;

import app.l2nx.gs.kafka.serde.GsonSerializer;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

class DefaultNxProducer implements NxProducer {

    private final KafkaProducer<String, Object> producer;
    private final NxLog log;

    DefaultNxProducer(Map<String, Object> config, Gson gson) {
        this.log = NxLogFactory.getLogger(DefaultNxProducer.class);
        this.producer = new KafkaProducer<>(config, new StringSerializer(), new GsonSerializer(gson));
        log.debug("Producer created");
    }

    @Override
    public void send(String topic, Object message) {
        try {
            producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to {}: {}", topic, exception.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void send(String topic, String key, Object message) {
        try {
            producer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to {}: {}", topic, exception.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void send(String topic, Object message, Callback callback) {
        try {
            producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}: {}", topic, e.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}: {}", topic, e.getMessage());
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}: {}", topic, callbackError.getMessage());
            }
        }
    }

    @Override
    public void send(String topic, String key, Object message, Callback callback) {
        try {
            producer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}: {}", topic, e.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}: {}", topic, e.getMessage());
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}: {}", topic, callbackError.getMessage());
            }
        }
    }

    @Override
    public void sendRecord(ProducerRecord<String, Object> record) {
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send record to {}: {}", record.topic(), exception.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send record to {}: {}", record.topic(), e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
            log.debug("Producer closed");
        } catch (Exception e) {
            log.warn("Error closing producer: {}", e.getMessage());
        }
    }
}
