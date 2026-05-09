package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WellKnownElementsTest {

    @Test
    void constants_shouldUseLowerCase() {
        assertEquals("fire", WellKnownElements.FIRE);
        assertEquals("water", WellKnownElements.WATER);
        assertEquals("earth", WellKnownElements.EARTH);
        assertEquals("wind", WellKnownElements.WIND);
        assertEquals("holy", WellKnownElements.HOLY);
        assertEquals("dark", WellKnownElements.DARK);
    }

    @Test
    void constants_shouldAllBeNonNull() {
        for (String value : allConstants()) {
            assertNotNull(value);
        }
    }

    @Test
    void constants_shouldHaveNoDuplicateValues() {
        List<String> values = allConstants();
        Set<String> deduped = new HashSet<String>(values);
        assertEquals(values.size(), deduped.size(),
                "WellKnownElements must not have two constants with the same string value");
    }

    private static List<String> allConstants() {
        return Arrays.asList(
                WellKnownElements.FIRE,
                WellKnownElements.WATER,
                WellKnownElements.EARTH,
                WellKnownElements.WIND,
                WellKnownElements.HOLY,
                WellKnownElements.DARK);
    }
}
