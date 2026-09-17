package app.l2nx.gs.adapter.api.kafka.commands.character;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SetCharacterAccessLevelCommandTest {

    @Test
    void builder_shouldRoundtripFields() {
        SetCharacterAccessLevelCommand cmd = SetCharacterAccessLevelCommand.builder()
                .charId(42L)
                .accessLevel("7")
                .staffNotes("promoted for event duty")
                .build();

        assertEquals(42L, cmd.getCharId().longValue());
        assertEquals("7", cmd.getAccessLevel());
        assertEquals("promoted for event duty", cmd.getStaffNotes());
    }

    @Test
    void constructor_shouldAcceptNullStaffNotes() {
        SetCharacterAccessLevelCommand cmd = new SetCharacterAccessLevelCommand(1L, "0", null);

        assertNull(cmd.getStaffNotes());
    }

    @Test
    void constructor_shouldRejectNullCharId() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new SetCharacterAccessLevelCommand(null, "7", null));
        assertTrue(ex.getMessage().contains("charId"));
    }

    @Test
    void constructor_shouldRejectNullAccessLevel() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new SetCharacterAccessLevelCommand(1L, null, null));
        assertTrue(ex.getMessage().contains("accessLevel"));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SetCharacterAccessLevelCommand original = SetCharacterAccessLevelCommand.builder()
                .charId(42L)
                .accessLevel("7")
                .staffNotes("note")
                .build();

        SetCharacterAccessLevelCommand copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCharId() {
        SetCharacterAccessLevelCommand a = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .build();
        SetCharacterAccessLevelCommand b = SetCharacterAccessLevelCommand.builder()
                .charId(2L)
                .accessLevel("7")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnAccessLevel() {
        SetCharacterAccessLevelCommand a = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .build();
        SetCharacterAccessLevelCommand b = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("0")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnStaffNotes() {
        SetCharacterAccessLevelCommand a = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .staffNotes("a")
                .build();
        SetCharacterAccessLevelCommand b = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .staffNotes("b")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        SetCharacterAccessLevelCommand a = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .staffNotes("note")
                .build();
        SetCharacterAccessLevelCommand b = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .staffNotes("note")
                .build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeFields() {
        SetCharacterAccessLevelCommand cmd = SetCharacterAccessLevelCommand.builder()
                .charId(42L)
                .accessLevel("7")
                .staffNotes("note")
                .build();

        String s = cmd.toString();

        assertTrue(s.contains("42"));
        assertTrue(s.contains("7"));
        assertTrue(s.contains("note"));
    }

    @Test
    void implementsNxCommandMarker() {
        SetCharacterAccessLevelCommand cmd = SetCharacterAccessLevelCommand.builder()
                .charId(1L)
                .accessLevel("7")
                .build();

        assertInstanceOf(app.l2nx.gs.adapter.api.kafka.commands.NxCommand.class, cmd);
    }
}
