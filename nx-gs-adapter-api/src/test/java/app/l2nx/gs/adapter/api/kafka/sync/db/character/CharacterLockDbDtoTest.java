package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CharacterLockDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        CharacterLockDbDto lock = CharacterLockDbDto.builder()
                .lockType(WellKnownCharacterLockTypes.IP)
                .lockValue("127.0.0.1")
                .build();

        assertEquals(WellKnownCharacterLockTypes.IP, lock.getLockType());
        assertEquals("127.0.0.1", lock.getLockValue());
    }

    @Test
    void lockValue_shouldBeNullable() {
        CharacterLockDbDto lock = CharacterLockDbDto.builder()
                .lockType(WellKnownCharacterLockTypes.HWID)
                .build();

        assertNull(lock.getLockValue());
    }

    @Test
    void build_shouldThrowNpe_whenLockTypeIsNull() {
        CharacterLockDbDto.Builder b = CharacterLockDbDto.builder().lockValue("x");

        assertThrows(NullPointerException.class, b::build);
    }

    @Test
    void equals_shouldDistinguishOnFields() {
        CharacterLockDbDto a = new CharacterLockDbDto(WellKnownCharacterLockTypes.IP, "127.0.0.1");
        CharacterLockDbDto b = new CharacterLockDbDto(WellKnownCharacterLockTypes.IP, "127.0.0.1");
        CharacterLockDbDto c = new CharacterLockDbDto(WellKnownCharacterLockTypes.IP, "10.0.0.1");
        CharacterLockDbDto d = new CharacterLockDbDto(WellKnownCharacterLockTypes.HWID, "127.0.0.1");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterLockDbDto original = new CharacterLockDbDto(WellKnownCharacterLockTypes.ITEM, "deadbeef");

        assertEquals(original, original.toBuilder().build());
    }
}
