package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomActivityTest {

    @Test
    void builder_shouldCarryTypeAndMetadata() {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(WellKnownCustomActivityMetadata.ELAPSED_SECONDS, "1820");
        meta.put(WellKnownCustomActivityMetadata.PENALTY_TIER, WellKnownCustomActivityMetadata.TIER_1);

        CustomActivity activity = CustomActivity.builder()
                .type(WellKnownCustomActivities.FISHING)
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
        CustomActivity activity = CustomActivity.builder()
                .type("fishing").metadata(meta).build();

        // Mutating the source map after build must not leak into the DTO.
        meta.put("elapsed_seconds", "999");
        assertEquals("10", activity.getMetadata().get("elapsed_seconds"));

        assertThrows(UnsupportedOperationException.class,
                () -> activity.getMetadata().put("x", "y"));
    }

    @Test
    void metadata_shouldBeNullable() {
        CustomActivity activity = CustomActivity.builder().type("reading").build();
        assertEquals("reading", activity.getType());
        assertNull(activity.getMetadata());
    }

    @Test
    void equalsHashCode_shouldReflectTypeAndMetadata() {
        CustomActivity a = CustomActivity.builder().type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "5")).build();
        CustomActivity b = CustomActivity.builder().type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "5")).build();
        CustomActivity c = CustomActivity.builder().type("fishing")
                .metadata(java.util.Collections.singletonMap("elapsed_seconds", "6")).build();

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CustomActivity original = CustomActivity.builder().type("fishing")
                .metadata(java.util.Collections.singletonMap("penalty_multiplier", "0.5")).build();

        assertEquals(original, original.toBuilder().build());
    }
}
