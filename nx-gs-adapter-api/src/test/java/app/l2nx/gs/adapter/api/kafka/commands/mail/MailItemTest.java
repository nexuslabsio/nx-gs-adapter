package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MailItemTest {

    @Test
    void builder_shouldRoundtripFields() {
        MailItem item = MailItem.builder()
                .itemTemplateId(57L)
                .count(1_000_000L)
                .build();

        assertEquals(57L, item.getItemTemplateId().longValue());
        assertEquals(1_000_000L, item.getCount().longValue());
    }

    @Test
    void builder_shouldDefaultCountToOne_whenNotSet() {
        MailItem item = MailItem.builder().itemTemplateId(57L).build();

        assertEquals(1L, item.getCount().longValue());
    }

    @Test
    void constructor_shouldRejectNullItemTemplateId() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MailItem(null, 1L));
        assertTrue(ex.getMessage().contains("itemTemplateId"));
    }

    @Test
    void constructor_shouldRejectNullCount() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MailItem(57L, null));
        assertTrue(ex.getMessage().contains("count"));
    }

    @Test
    void constructor_shouldRejectZeroCount() {
        assertThrows(IllegalArgumentException.class, () -> new MailItem(57L, 0L));
    }

    @Test
    void constructor_shouldRejectNegativeCount() {
        assertThrows(IllegalArgumentException.class, () -> new MailItem(57L, -1L));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        MailItem original = MailItem.builder().itemTemplateId(57L).count(42L).build();
        MailItem copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnTemplateId() {
        MailItem a = MailItem.builder().itemTemplateId(57L).count(1L).build();
        MailItem b = MailItem.builder().itemTemplateId(58L).count(1L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnCount() {
        MailItem a = MailItem.builder().itemTemplateId(57L).count(1L).build();
        MailItem b = MailItem.builder().itemTemplateId(57L).count(2L).build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        MailItem a = MailItem.builder().itemTemplateId(57L).count(42L).build();
        MailItem b = MailItem.builder().itemTemplateId(57L).count(42L).build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeAllFields() {
        MailItem item = MailItem.builder().itemTemplateId(57L).count(42L).build();

        String s = item.toString();

        assertTrue(s.contains("57"));
        assertTrue(s.contains("42"));
    }
}
