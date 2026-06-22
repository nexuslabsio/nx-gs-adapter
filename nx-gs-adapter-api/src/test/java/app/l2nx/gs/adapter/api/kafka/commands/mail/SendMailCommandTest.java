package app.l2nx.gs.adapter.api.kafka.commands.mail;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class SendMailCommandTest {

    @Test
    void builder_shouldRoundtripFields() {
        MailItem item = MailItem.builder().itemTemplateId(57L).count(1_000L).build();

        SendMailCommand cmd = SendMailCommand.builder()
                .charId(42L)
                .author("admin")
                .title("Reward")
                .body("here is your loot")
                .items(Collections.singletonList(item))
                .build();

        assertEquals(42L, cmd.getCharId().longValue());
        assertEquals("admin", cmd.getAuthor());
        assertEquals("Reward", cmd.getTitle());
        assertEquals("here is your loot", cmd.getBody());
        assertEquals(Collections.singletonList(item), cmd.getItems());
    }

    @Test
    void constructor_shouldRejectNullCharId() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new SendMailCommand(null, "a", "t", "b", null));
        assertTrue(ex.getMessage().contains("charId"));
    }

    @Test
    void constructor_shouldRejectNullTitle() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new SendMailCommand(1L, "a", null, "b", null));
        assertTrue(ex.getMessage().contains("title"));
    }

    @Test
    void constructor_shouldAllowNullAuthor() {
        SendMailCommand cmd = new SendMailCommand(1L, null, "t", "b", null);

        assertNull(cmd.getAuthor());
    }

    @Test
    void constructor_shouldAllowNullBody() {
        SendMailCommand cmd = new SendMailCommand(1L, "a", "t", null, null);

        assertNull(cmd.getBody());
    }

    @Test
    void getItems_shouldReturnEmpty_whenNull() {
        SendMailCommand cmd = new SendMailCommand(1L, "a", "t", "b", null);

        assertTrue(cmd.getItems().isEmpty());
    }

    @Test
    void getItems_shouldBeUnmodifiable() {
        MailItem item = MailItem.builder().itemTemplateId(57L).build();
        SendMailCommand cmd = SendMailCommand.builder()
                .charId(1L)
                .title("t")
                .items(Collections.singletonList(item))
                .build();

        assertThrows(UnsupportedOperationException.class, () -> cmd.getItems().add(item));
    }

    @Test
    void constructor_shouldDefensivelyCopyItems() {
        MailItem item = MailItem.builder().itemTemplateId(57L).build();
        java.util.List<MailItem> mutable = new java.util.ArrayList<MailItem>(Collections.singletonList(item));

        SendMailCommand cmd = new SendMailCommand(1L, "a", "t", "b", mutable);
        mutable.add(MailItem.builder().itemTemplateId(58L).build());

        assertEquals(1, cmd.getItems().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SendMailCommand original = SendMailCommand.builder()
                .charId(42L)
                .author("admin")
                .title("Reward")
                .body("body")
                .items(Arrays.asList(
                        MailItem.builder().itemTemplateId(57L).count(1L).build(),
                        MailItem.builder().itemTemplateId(58L).count(2L).build()))
                .build();

        SendMailCommand copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCharId() {
        SendMailCommand a = SendMailCommand.builder().charId(1L).title("t").build();
        SendMailCommand b = SendMailCommand.builder().charId(2L).title("t").build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnTitle() {
        SendMailCommand a = SendMailCommand.builder().charId(1L).title("a").build();
        SendMailCommand b = SendMailCommand.builder().charId(1L).title("b").build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnAuthor() {
        SendMailCommand a =
                SendMailCommand.builder().charId(1L).title("t").author("x").build();
        SendMailCommand b =
                SendMailCommand.builder().charId(1L).title("t").author("y").build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnItems() {
        SendMailCommand a = SendMailCommand.builder()
                .charId(1L)
                .title("t")
                .items(Collections.singletonList(
                        MailItem.builder().itemTemplateId(57L).build()))
                .build();
        SendMailCommand b = SendMailCommand.builder()
                .charId(1L)
                .title("t")
                .items(Collections.singletonList(
                        MailItem.builder().itemTemplateId(58L).build()))
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        SendMailCommand a = SendMailCommand.builder()
                .charId(1L)
                .author("x")
                .title("t")
                .body("b")
                .build();
        SendMailCommand b = SendMailCommand.builder()
                .charId(1L)
                .author("x")
                .title("t")
                .body("b")
                .build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeAllFields() {
        SendMailCommand cmd = SendMailCommand.builder()
                .charId(42L)
                .author("admin")
                .title("Reward")
                .body("body")
                .build();

        String s = cmd.toString();

        assertTrue(s.contains("42"));
        assertTrue(s.contains("admin"));
        assertTrue(s.contains("Reward"));
        assertTrue(s.contains("body"));
    }

    @Test
    void implementsNxCommandMarker() {
        SendMailCommand cmd = SendMailCommand.builder().charId(1L).title("t").build();

        assertInstanceOf(NxCommand.class, cmd);
    }
}
