package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ItemDeliveryErrorTest {

    @Test
    void builder_shouldRoundtripFields() {
        ItemDeliveryError err = ItemDeliveryError.builder()
                .itemTemplateId(57L)
                .count(1L)
                .reason("template not found")
                .build();

        assertEquals(57L, err.getItemTemplateId().longValue());
        assertEquals(1L, err.getCount().longValue());
        assertEquals("template not found", err.getReason());
    }

    @Test
    void getReason_shouldReturnEmptyString_whenNull() {
        ItemDeliveryError err = ItemDeliveryError.builder().reason(null).build();

        assertEquals("", err.getReason());
    }

    @Test
    void constructor_shouldAllowNullItemTemplateId() {
        ItemDeliveryError err = new ItemDeliveryError(null, 1L, "x");

        assertNull(err.getItemTemplateId());
    }

    @Test
    void constructor_shouldAllowNullCount() {
        ItemDeliveryError err = new ItemDeliveryError(57L, null, "x");

        assertNull(err.getCount());
    }

    @Test
    void constructor_shouldAllowAllNullsExceptStoredReason() {
        ItemDeliveryError err = new ItemDeliveryError(null, null, null);

        assertNull(err.getItemTemplateId());
        assertNull(err.getCount());
        assertEquals("", err.getReason());
    }

    @Test
    void equals_shouldTreatNullReasonAndEmptyReasonAsEqual() {
        ItemDeliveryError fromNull = new ItemDeliveryError(57L, 1L, null);
        ItemDeliveryError fromEmpty = new ItemDeliveryError(57L, 1L, "");

        assertEquals(fromNull, fromEmpty);
        assertEquals(fromNull.hashCode(), fromEmpty.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ItemDeliveryError original = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(2L).reason("boom").build();
        ItemDeliveryError copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnReason() {
        ItemDeliveryError a = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("a").build();
        ItemDeliveryError b = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("b").build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        ItemDeliveryError a = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("x").build();
        ItemDeliveryError b = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("x").build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeAllFields() {
        ItemDeliveryError err = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("x").build();

        String s = err.toString();

        assertTrue(s.contains("57"));
        assertTrue(s.contains("x"));
    }
}
