package app.l2nx.gs.adapter.api.kafka.commands.character;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SetCharacterAccessLevelResultTest {

    @Test
    void builder_shouldRoundtripFields() {
        SetCharacterAccessLevelResult result = SetCharacterAccessLevelResult.builder()
                .charId(42L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .wasOnline(true)
                .build();

        assertEquals(42L, result.getCharId().longValue());
        assertEquals("7", result.getAccessLevel());
        assertEquals("0", result.getPreviousAccessLevel());
        assertTrue(result.isWasOnline());
    }

    @Test
    void constructor_shouldAcceptNullPreviousAccessLevel() {
        SetCharacterAccessLevelResult result = new SetCharacterAccessLevelResult(1L, "7", null, false);

        assertNull(result.getPreviousAccessLevel());
    }

    @Test
    void constructor_shouldRejectNullCharId() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> new SetCharacterAccessLevelResult(null, "7", null, false));
        assertTrue(ex.getMessage().contains("charId"));
    }

    @Test
    void constructor_shouldRejectNullAccessLevel() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> new SetCharacterAccessLevelResult(1L, null, null, false));
        assertTrue(ex.getMessage().contains("accessLevel"));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SetCharacterAccessLevelResult original = SetCharacterAccessLevelResult.builder()
                .charId(42L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .wasOnline(true)
                .build();

        SetCharacterAccessLevelResult copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCharId() {
        SetCharacterAccessLevelResult a = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .build();
        SetCharacterAccessLevelResult b = SetCharacterAccessLevelResult.builder()
                .charId(2L)
                .accessLevel("7")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnAccessLevel() {
        SetCharacterAccessLevelResult a = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .build();
        SetCharacterAccessLevelResult b = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("0")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnPreviousAccessLevel() {
        SetCharacterAccessLevelResult a = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .build();
        SetCharacterAccessLevelResult b = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .previousAccessLevel("1")
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnWasOnline() {
        SetCharacterAccessLevelResult a = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .wasOnline(true)
                .build();
        SetCharacterAccessLevelResult b = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .wasOnline(false)
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        SetCharacterAccessLevelResult a = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .wasOnline(true)
                .build();
        SetCharacterAccessLevelResult b = SetCharacterAccessLevelResult.builder()
                .charId(1L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .wasOnline(true)
                .build();

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toString_shouldExposeFields() {
        SetCharacterAccessLevelResult result = SetCharacterAccessLevelResult.builder()
                .charId(42L)
                .accessLevel("7")
                .previousAccessLevel("0")
                .wasOnline(true)
                .build();

        String s = result.toString();

        assertTrue(s.contains("42"));
        assertTrue(s.contains("7"));
        assertTrue(s.contains("0"));
        assertTrue(s.contains("true"));
    }
}
