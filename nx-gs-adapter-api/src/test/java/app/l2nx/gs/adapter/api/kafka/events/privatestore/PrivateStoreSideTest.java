package app.l2nx.gs.adapter.api.kafka.events.privatestore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PrivateStoreSideTest {

    @Test
    void values_shouldExposeAskAndBidOnly() {
        assertArrayEquals(
                new PrivateStoreSide[] {PrivateStoreSide.ASK, PrivateStoreSide.BID}, PrivateStoreSide.values());
    }

    @Test
    void valueOf_shouldRoundtripBothConstants() {
        assertEquals(PrivateStoreSide.ASK, PrivateStoreSide.valueOf("ASK"));
        assertEquals(PrivateStoreSide.BID, PrivateStoreSide.valueOf("BID"));
    }

    @Test
    void name_shouldMatchEnumLiteral() {
        // Wire reflects the enum literal (Gson default) — pin the format so a
        // refactor renaming the constants surfaces here, not on the platform side.
        assertEquals("ASK", PrivateStoreSide.ASK.name());
        assertEquals("BID", PrivateStoreSide.BID.name());
    }
}
