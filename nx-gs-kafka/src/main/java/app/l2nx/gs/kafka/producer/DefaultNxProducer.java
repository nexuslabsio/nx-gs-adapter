package app.l2nx.gs.kafka.producer;

import app.l2nx.gs.kafka.serde.GsonSerializer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class DefaultNxProducer implements NxProducer {

    private static final Header[] EMPTY_HEADERS = new Header[0];

    private final KafkaProducer<String, Object> producer;
    private final KafkaProducer<byte[], Object> bytesKeyProducer;
    private final Header[] staticHeaders;
    private final NxLog log;

    DefaultNxProducer(Map<String, Object> config, Gson gson) {
        this(config, gson, Collections.emptyMap());
    }

    DefaultNxProducer(Map<String, Object> config, Gson gson, Map<String, byte[]> staticHeaders) {
        this.log = NxLogFactory.getLogger(DefaultNxProducer.class);
        this.producer = new KafkaProducer<>(config, new StringSerializer(), new GsonSerializer(gson));
        // Second producer dedicated to byte[]-keyed sends (e.g. CDC primitive-PK keying).
        // Shares the same broker config; only the key serializer differs.
        this.bytesKeyProducer = new KafkaProducer<>(config, new ByteArraySerializer(), new GsonSerializer(gson));
        this.staticHeaders = buildStaticHeaders(staticHeaders);
        if (this.staticHeaders.length > 0) {
            log.debug("Producer created with {} static header(s)", this.staticHeaders.length);
        }
    }

    private static Header[] buildStaticHeaders(Map<String, byte[]> staticHeaders) {
        if (staticHeaders == null || staticHeaders.isEmpty()) {
            return EMPTY_HEADERS;
        }
        List<Header> built = new ArrayList<>(staticHeaders.size());
        for (Map.Entry<String, byte[]> e : staticHeaders.entrySet()) {
            built.add(new RecordHeader(e.getKey(), e.getValue()));
        }
        return built.toArray(new Header[0]);
    }

    private void stamp(Headers headers) {
        for (Header h : staticHeaders) {
            headers.add(h);
        }
    }

    @Override
    public void send(String topic, Object message) {
        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
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
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
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
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
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
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
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
    public void send(String topic, byte[] key, Object message, Callback callback) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, key, message);
            stamp(record.headers());
            bytesKeyProducer.send(record, (metadata, exception) -> {
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
            stamp(record.headers());
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
    public void sendBytesKeyRecord(ProducerRecord<byte[], Object> record, Callback callback) {
        try {
            stamp(record.headers());
            bytesKeyProducer.send(record, (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}: {}", record.topic(), e.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send record to {}: {}", record.topic(), e.getMessage());
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}: {}", record.topic(), callbackError.getMessage());
            }
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("Error closing string-key producer: {}", e.getMessage());
        }
        try {
            bytesKeyProducer.close();
        } catch (Exception e) {
            log.warn("Error closing bytes-key producer: {}", e.getMessage());
        }
        log.debug("Producers closed");
    }
}
