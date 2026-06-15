package app.l2nx.gs.kafka.producer;

import app.l2nx.gs.kafka.serde.GsonSerializer;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class DefaultNxProducer implements NxProducer {

    private static final Header[] EMPTY_HEADERS = new Header[0];
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<byte[], Object> producer;
    private final Header[] staticHeaders;
    private final Duration closeTimeout;
    private final NxLog log;

    DefaultNxProducer(Map<String, Object> config, Gson gson) {
        this(config, gson, Collections.emptyMap(), DEFAULT_CLOSE_TIMEOUT);
    }

    DefaultNxProducer(Map<String, Object> config, Gson gson, Map<String, byte[]> staticHeaders) {
        this(config, gson, staticHeaders, DEFAULT_CLOSE_TIMEOUT);
    }

    DefaultNxProducer(Map<String, Object> config, Gson gson, Map<String, byte[]> staticHeaders,
                      Duration closeTimeout) {
        this(new KafkaProducer<>(config, new ByteArraySerializer(), new GsonSerializer(gson)),
                staticHeaders, closeTimeout);
    }

    DefaultNxProducer(Producer<byte[], Object> producer, Map<String, byte[]> staticHeaders,
                      Duration closeTimeout) {
        this.log = NxLogFactory.getLogger(DefaultNxProducer.class);
        this.producer = producer;
        this.staticHeaders = buildStaticHeaders(staticHeaders);
        this.closeTimeout = closeTimeout;
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
        // Fresh RecordHeader per record — sharing one instance across records leaks state via Headers' internal list.
        for (Header h : staticHeaders) {
            headers.add(new RecordHeader(h.key(), h.value()));
        }
    }

    private static byte[] encodeKey(String key) {
        return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void send(String topic, Object message) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, null, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to {}", topic, exception);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
        }
    }

    @Override
    public void send(String topic, String key, Object message) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, encodeKey(key), message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to {}", topic, exception);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
        }
    }

    @Override
    public void send(String topic, Object message, Callback callback) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, null, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}", topic, e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}", topic, callbackError);
            }
        }
    }

    @Override
    public void send(String topic, String key, Object message, Callback callback) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, encodeKey(key), message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}", topic, e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}", topic, callbackError);
            }
        }
    }

    @Override
    public void send(String topic, byte[] key, Object message, Callback callback) {
        try {
            ProducerRecord<byte[], Object> record = new ProducerRecord<>(topic, key, message);
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}", topic, e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}", topic, callbackError);
            }
        }
    }

    @Override
    public void sendRecord(ProducerRecord<String, Object> record) {
        try {
            ProducerRecord<byte[], Object> bytesRecord = new ProducerRecord<>(
                    record.topic(), record.partition(), record.timestamp(),
                    encodeKey(record.key()), record.value(), record.headers());
            stamp(bytesRecord.headers());
            producer.send(bytesRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send record to {}", record.topic(), exception);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send record to {}", record.topic(), e);
        }
    }

    @Override
    public void sendBytesKeyRecord(ProducerRecord<byte[], Object> record, Callback callback) {
        try {
            stamp(record.headers());
            producer.send(record, (metadata, exception) -> {
                try {
                    callback.onCompletion(metadata, exception);
                } catch (Exception e) {
                    log.error("Callback error for topic {}", record.topic(), e);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send record to {}", record.topic(), e);
            try {
                callback.onCompletion(null, e);
            } catch (Exception callbackError) {
                log.error("Callback error for topic {}", record.topic(), callbackError);
            }
        }
    }

    @Override
    public void flush() {
        try {
            producer.flush();
        } catch (Exception e) {
            log.warn("Error flushing producer", e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close(closeTimeout);
        } catch (Exception e) {
            log.warn("Error closing producer", e);
        }
        log.debug("Producer closed");
    }
}
