package app.l2nx.gs.adapter.api.kafka.events.online;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WellKnownOnlineBucketsTest {

    @Test
    void constants_shouldUseLowerSnakeCase() {
        assertEquals("total", WellKnownOnlineBuckets.TOTAL);
        assertEquals("online", WellKnownOnlineBuckets.ONLINE);
        assertEquals("real", WellKnownOnlineBuckets.REAL);
        assertEquals("offline_trade", WellKnownOnlineBuckets.OFFLINE_TRADE);
        assertEquals("fishing", WellKnownOnlineBuckets.FISHING);
        assertEquals("phantoms", WellKnownOnlineBuckets.PHANTOMS);
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
                "WellKnownOnlineBuckets must not have two constants with the same string value");
    }

    private static List<String> allConstants() {
        return Arrays.asList(
                WellKnownOnlineBuckets.TOTAL,
                WellKnownOnlineBuckets.ONLINE,
                WellKnownOnlineBuckets.REAL,
                WellKnownOnlineBuckets.OFFLINE_TRADE,
                WellKnownOnlineBuckets.FISHING,
                WellKnownOnlineBuckets.PHANTOMS);
    }
}
