package app.l2nx.gs.adapter.api.kafka.events.serveronline;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WellKnownServerOnlineBucketsTest {

    @Test
    void constants_shouldUseLowerSnakeCase() {
        assertEquals("total", WellKnownServerOnlineBuckets.TOTAL);
        assertEquals("online", WellKnownServerOnlineBuckets.ONLINE);
        assertEquals("real", WellKnownServerOnlineBuckets.REAL);
        assertEquals("offline_trade", WellKnownServerOnlineBuckets.OFFLINE_TRADE);
        assertEquals("fishing", WellKnownServerOnlineBuckets.FISHING);
        assertEquals("phantoms", WellKnownServerOnlineBuckets.PHANTOMS);
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
                "WellKnownServerOnlineBuckets must not have two constants with the same string value");
    }

    private static List<String> allConstants() {
        return Arrays.asList(
                WellKnownServerOnlineBuckets.TOTAL,
                WellKnownServerOnlineBuckets.ONLINE,
                WellKnownServerOnlineBuckets.REAL,
                WellKnownServerOnlineBuckets.OFFLINE_TRADE,
                WellKnownServerOnlineBuckets.FISHING,
                WellKnownServerOnlineBuckets.PHANTOMS);
    }
}
