package app.l2nx.gs.kafka;

import com.google.gson.*;

import java.time.Duration;
import java.time.Instant;

/**
 * Pre-configured {@link Gson} factory for nx-gs wire payloads. Registers
 * type adapters for {@link Instant} and {@link Duration} so Gson emits
 * ISO-8601 strings (e.g. {@code "2026-05-17T12:00:00Z"} /
 * {@code "PT60S"}) instead of its default struct form.
 *
 * <p>Use this Gson instance for any Kafka publisher / consumer that serializes
 * nx-gs adapter wire DTOs — {@code DefaultKafkaFactory} wires it into
 * {@code NxKafka.configure().gson(...)} for the singleton.</p>
 */
public final class NxGsonAdapters {

    private NxGsonAdapters() {
    }

    /**
     * Returns a fresh {@link Gson} configured with the standard nx-gs adapters.
     * Builders may further customize via {@link #builder()}.
     */
    public static Gson defaultGson() {
        return builder().create();
    }

    /**
     * Returns a {@link GsonBuilder} pre-registered with the standard
     * adapters, ready for additional customization before {@code .create()}.
     */
    public static GsonBuilder builder() {
        JsonSerializer<Instant> instantSer = (src, typeOfSrc, ctx) -> new JsonPrimitive(src.toString());
        JsonDeserializer<Instant> instantDe = (json, typeOfT, ctx) -> Instant.parse(json.getAsString());
        JsonSerializer<Duration> durationSer = (src, typeOfSrc, ctx) -> new JsonPrimitive(src.toString());
        JsonDeserializer<Duration> durationDe = (json, typeOfT, ctx) -> Duration.parse(json.getAsString());
        return new GsonBuilder()
                .disableHtmlEscaping()
                .registerTypeAdapter(Instant.class, instantSer)
                .registerTypeAdapter(Instant.class, instantDe)
                .registerTypeAdapter(Duration.class, durationSer)
                .registerTypeAdapter(Duration.class, durationDe);
    }
}
