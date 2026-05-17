package app.l2nx.gs.kafka;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NxGsonAdaptersTest {

    @Test
    void instant_shouldSerializeAsIso8601ZSuffix() {
        Gson gson = NxGsonAdapters.defaultGson();
        Instant instant = Instant.parse("2026-05-17T12:00:00Z");

        String json = gson.toJson(instant);

        assertEquals("\"2026-05-17T12:00:00Z\"", json);
    }

    @Test
    void instant_shouldRoundtrip() {
        Gson gson = NxGsonAdapters.defaultGson();
        Instant original = Instant.parse("2026-05-17T12:34:56.789Z");

        Instant decoded = gson.fromJson(gson.toJson(original), Instant.class);

        assertEquals(original, decoded);
    }

    @Test
    void duration_shouldSerializeAsIso8601() {
        Gson gson = NxGsonAdapters.defaultGson();

        assertEquals("\"PT1M\"", gson.toJson(Duration.ofSeconds(60)));
        assertEquals("\"PT1H30M\"", gson.toJson(Duration.ofMinutes(90)));
        assertEquals("\"PT0S\"", gson.toJson(Duration.ZERO));
    }

    @Test
    void duration_shouldRoundtrip() {
        Gson gson = NxGsonAdapters.defaultGson();
        Duration original = Duration.ofHours(2).plusMinutes(15).plusSeconds(30);

        Duration decoded = gson.fromJson(gson.toJson(original), Duration.class);

        assertEquals(original, decoded);
    }

    @Test
    void dtoWithInstantAndDuration_shouldRoundtrip_asNestedFields() {
        Gson gson = NxGsonAdapters.defaultGson();
        Wrapper original = new Wrapper(
                Instant.parse("2026-05-17T12:00:00Z"),
                Duration.ofSeconds(45));

        String json = gson.toJson(original);
        Wrapper decoded = gson.fromJson(json, Wrapper.class);

        assertTrue(json.contains("\"observedAt\":\"2026-05-17T12:00:00Z\""), json);
        assertTrue(json.contains("\"elapsed\":\"PT45S\""), json);
        assertEquals(original.observedAt, decoded.observedAt);
        assertEquals(original.elapsed, decoded.elapsed);
    }

    private static final class Wrapper {
        final Instant observedAt;
        final Duration elapsed;

        Wrapper(Instant observedAt, Duration elapsed) {
            this.observedAt = observedAt;
            this.elapsed = elapsed;
        }
    }
}
