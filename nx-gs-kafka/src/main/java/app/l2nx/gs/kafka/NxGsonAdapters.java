package app.l2nx.gs.kafka;

import com.google.gson.*;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;

/**
 * ISO-8601 adapters for Instant, Duration and OffsetTime; Gson would otherwise emit its default struct form.
 */
public final class NxGsonAdapters {

    private NxGsonAdapters() {}

    public static Gson defaultGson() {
        return builder().create();
    }

    public static GsonBuilder builder() {
        JsonSerializer<Instant> instantSer = (src, typeOfSrc, ctx) -> new JsonPrimitive(src.toString());
        JsonDeserializer<Instant> instantDe = (json, typeOfT, ctx) -> Instant.parse(json.getAsString());
        JsonSerializer<Duration> durationSer = (src, typeOfSrc, ctx) -> new JsonPrimitive(src.toString());
        JsonDeserializer<Duration> durationDe = (json, typeOfT, ctx) -> Duration.parse(json.getAsString());
        // Fixed HH:mm:ssXXX: toString() drops zero seconds, so the wire format would vary; parsing stays lenient
        DateTimeFormatter offsetTimeFmt = DateTimeFormatter.ofPattern("HH:mm:ssXXX");
        JsonSerializer<OffsetTime> offsetTimeSer =
                (src, typeOfSrc, ctx) -> new JsonPrimitive(src.format(offsetTimeFmt));
        JsonDeserializer<OffsetTime> offsetTimeDe = (json, typeOfT, ctx) -> OffsetTime.parse(json.getAsString());
        return new GsonBuilder()
                .disableHtmlEscaping()
                .registerTypeAdapter(Instant.class, instantSer)
                .registerTypeAdapter(Instant.class, instantDe)
                .registerTypeAdapter(Duration.class, durationSer)
                .registerTypeAdapter(Duration.class, durationDe)
                .registerTypeAdapter(OffsetTime.class, offsetTimeSer)
                .registerTypeAdapter(OffsetTime.class, offsetTimeDe);
    }
}
