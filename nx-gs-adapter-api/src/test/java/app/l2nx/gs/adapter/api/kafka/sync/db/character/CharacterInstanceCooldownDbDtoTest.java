package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class CharacterInstanceCooldownDbDtoTest {

    private static final Instant REENTRY = Instant.parse("2026-07-02T00:00:00Z");

    @Test
    void builder_shouldMatchConstructor() {
        CharacterInstanceCooldownDbDto fromBuilder = CharacterInstanceCooldownDbDto.builder()
                .instanceId(112)
                .reentryAt(REENTRY)
                .build();
        CharacterInstanceCooldownDbDto fromCtor = new CharacterInstanceCooldownDbDto(112, REENTRY);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void equals_shouldDistinguishInstanceId() {
        assertNotEquals(new CharacterInstanceCooldownDbDto(1, REENTRY), new CharacterInstanceCooldownDbDto(2, REENTRY));
    }

    @Test
    void constructor_shouldRejectNullReentryAt() {
        assertThrows(NullPointerException.class, () -> new CharacterInstanceCooldownDbDto(1, null));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterInstanceCooldownDbDto original = new CharacterInstanceCooldownDbDto(42, REENTRY);

        assertEquals(original, original.toBuilder().build());
    }
}
