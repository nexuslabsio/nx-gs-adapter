package app.l2nx.gs.adapter.api.kafka.commands.item;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeleteItemCommandTest {

    @Test
    void builder_shouldRoundtripFields() {
        DeleteItemCommand cmd = DeleteItemCommand.builder()
                .charId(42L)
                .itemId(1234567L)
                .count(7L)
                .build();

        assertEquals(42L, cmd.getCharId().longValue());
        assertEquals(1234567L, cmd.getItemId().longValue());
        assertEquals(7L, cmd.getCount().longValue());
    }

    @Test
    void builder_shouldDefaultCountToOne_whenNotSet() {
        DeleteItemCommand cmd = DeleteItemCommand.builder()
                .charId(42L)
                .itemId(1234567L)
                .build();

        assertEquals(1L, cmd.getCount().longValue());
    }

    @Test
    void constructor_shouldRejectNullCharId() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new DeleteItemCommand(null, 1L, 1L));
        assertTrue(ex.getMessage().contains("charId"));
    }

    @Test
    void constructor_shouldRejectNullItemId() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new DeleteItemCommand(1L, null, 1L));
        assertTrue(ex.getMessage().contains("itemId"));
    }

    @Test
    void constructor_shouldRejectNullCount() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new DeleteItemCommand(1L, 1L, null));
        assertTrue(ex.getMessage().contains("count"));
    }

    @Test
    void constructor_shouldRejectZeroCount() {
        assertThrows(IllegalArgumentException.class,
                () -> new DeleteItemCommand(1L, 1L, 0L));
    }

    @Test
    void constructor_shouldRejectNegativeCount() {
        assertThrows(IllegalArgumentException.class,
                () -> new DeleteItemCommand(1L, 1L, -5L));
    }

    @Test
    void builder_buildWithoutRequiredField_shouldThrow() {
        // charId not set → null → constructor rejects
        assertThrows(IllegalArgumentException.class,
                () -> DeleteItemCommand.builder().itemId(1L).build());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        DeleteItemCommand original = DeleteItemCommand.builder()
                .charId(42L)
                .itemId(1234567L)
                .count(3L)
                .build();

        DeleteItemCommand copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCharId() {
        DeleteItemCommand a = DeleteItemCommand.builder().charId(1L).itemId(99L).build();
        DeleteItemCommand b = DeleteItemCommand.builder().charId(2L).itemId(99L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnItemId() {
        DeleteItemCommand a = DeleteItemCommand.builder().charId(1L).itemId(99L).build();
        DeleteItemCommand b = DeleteItemCommand.builder().charId(1L).itemId(100L).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnCount() {
        DeleteItemCommand a = DeleteItemCommand.builder().charId(1L).itemId(99L).count(1L).build();
        DeleteItemCommand b = DeleteItemCommand.builder().charId(1L).itemId(99L).count(5L).build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        DeleteItemCommand a = DeleteItemCommand.builder().charId(1L).itemId(99L).count(3L).build();
        DeleteItemCommand b = DeleteItemCommand.builder().charId(1L).itemId(99L).count(3L).build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeAllFields() {
        DeleteItemCommand cmd = DeleteItemCommand.builder()
                .charId(42L)
                .itemId(1234567L)
                .count(7L)
                .build();

        String s = cmd.toString();

        assertTrue(s.contains("42"));
        assertTrue(s.contains("1234567"));
        assertTrue(s.contains("7"));
    }

    @Test
    void implementsNxCommandMarker() {
        DeleteItemCommand cmd = DeleteItemCommand.builder().charId(1L).itemId(2L).build();

        assertInstanceOf(app.l2nx.gs.adapter.api.kafka.commands.NxCommand.class, cmd);
    }
}
