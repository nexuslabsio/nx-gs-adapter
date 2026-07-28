package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ActivityTest {

    @Test
    void builder_shouldCarryTypeAndMetadata() {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(WellKnownActivityMetadata.ELAPSED_SECONDS, "1820");
        meta.put(WellKnownActivityMetadata.PENALTY_TIER, WellKnownActivityMetadata.TIER_1);

        Activity activity = Activity.builder()
                .type(WellKnownActivities.FISHING)
                .metadata(meta)
                .build();

        assertEquals("fishing", activity.getType());
        assertEquals("1820", activity.getMetadata().get("elapsed_seconds"));
        assertEquals("tier1", activity.getMetadata().get("penalty_tier"));
    }

    @Test
    void metadata_shouldBeDefensivelyCopiedAndUnmodifiable() {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("elapsed_seconds", "10");
        Activity activity = Activity.builder().type("fishing").metadata(meta).build();

        // Mutating the source map after build must not leak into the DTO.
        meta.put("elapsed_seconds", "999");
        assertEquals("10", activity.getMetadata().get("elapsed_seconds"));

        assertThrows(
                UnsupportedOperationException.class,
                () -> activity.getMetadata().put("x", "y"));
    }

    @Test
    void metadata_shouldBeNullable() {
        Activity activity = Activity.builder().type("reading").build();
        assertEquals("reading", activity.getType());
        assertNull(activity.getMetadata());
    }

    @Test
    void equalsHashCode_shouldReflectTypeAndMetadata() {
        Activity a = Activity.builder()
                .type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "5"))
                .build();
        Activity b = Activity.builder()
                .type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "5"))
                .build();
        Activity c = Activity.builder()
                .type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "6"))
                .build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        Activity original = Activity.builder()
                .type("fishing")
                .metadata(java.util.Collections.singletonMap("penalty_multiplier", "0.5"))
                .build();

        assertEquals(original, original.toBuilder().build());
    }
}
