package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParentRefTest {

    @Test
    void of_shouldRoundtripFields() {
        ParentRef ref = ParentRef.of("character", "owner_id");

        assertEquals("character", ref.parentEntityName());
        assertEquals("owner_id", ref.fkColumn());
    }

    @Test
    void of_shouldRejectNullParentEntityName() {
        assertThrows(NullPointerException.class, () -> ParentRef.of(null, "owner_id"));
    }

    @Test
    void of_shouldRejectNullFkColumn() {
        assertThrows(NullPointerException.class, () -> ParentRef.of("character", null));
    }

    @Test
    void equals_shouldMatchOnBothFields() {
        assertEquals(ParentRef.of("character", "owner_id"), ParentRef.of("character", "owner_id"));
        assertNotEquals(ParentRef.of("character", "owner_id"), ParentRef.of("clan", "owner_id"));
        assertNotEquals(ParentRef.of("character", "owner_id"), ParentRef.of("character", "clan_id"));
    }

    @Test
    void hashCode_shouldMatchEquals() {
        assertEquals(ParentRef.of("character", "owner_id").hashCode(),
                ParentRef.of("character", "owner_id").hashCode());
    }

    @Test
    void toString_shouldExposeBothFields() {
        String s = ParentRef.of("character", "owner_id").toString();

        assertTrue(s.contains("character"));
        assertTrue(s.contains("owner_id"));
    }
}
