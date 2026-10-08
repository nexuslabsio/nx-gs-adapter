package app.l2nx.gs.adapter.api.spi.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RuntimeRowTest {

    @Test
    void constructor_shouldDefaultStateStampToZero_whenNotGiven() {
        assertEquals(0L, new RuntimeRow<String>(1L, "dto").getStateStamp());
    }

    @Test
    void equals_shouldDiffer_whenOnlyStateStampDiffers() {
        RuntimeRow<String> base = new RuntimeRow<String>(1L, "dto", 7L);

        assertNotEquals(base, new RuntimeRow<String>(1L, "dto", 8L));
        assertEquals(base, new RuntimeRow<String>(1L, "dto", 7L));
        assertEquals(base.hashCode(), new RuntimeRow<String>(1L, "dto", 7L).hashCode());
    }
}
