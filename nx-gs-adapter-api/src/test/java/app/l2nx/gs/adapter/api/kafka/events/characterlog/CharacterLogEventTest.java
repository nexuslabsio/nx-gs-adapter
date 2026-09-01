package app.l2nx.gs.adapter.api.kafka.events.characterlog;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class CharacterLogEventTest {

    private static CharacterLogEvent.Builder valid() {
        return CharacterLogEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(268476304L)
                .type(WellKnownCharacterLogTypes.SECOND_CLASS);
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(
                NullPointerException.class,
                () -> CharacterLogEvent.builder()
                        .charId(1L)
                        .type(WellKnownCharacterLogTypes.NOBLESSE)
                        .build());
    }

    @Test
    void constructor_shouldRejectNullType() {
        assertThrows(
                NullPointerException.class,
                () -> CharacterLogEvent.builder()
                        .eventId(UUID.randomUUID())
                        .charId(1L)
                        .build());
    }

    @Test
    void getMetadata_shouldReturnNull_whenBuilderOmits() {
        assertNull(valid().build().getMetadata());
    }

    @Test
    void getMetadata_shouldBeUnmodifiable() {
        CharacterLogEvent event = valid().metadata(
                        Collections.singletonMap(WellKnownCharacterLogMetadata.CLASS_LEVEL, "2"))
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> event.getMetadata().put(WellKnownCharacterLogMetadata.CHAR_LEVEL, "40"));
    }

    @Test
    void getMetadata_shouldNotSeeLaterMutationsOfTheSourceMap() {
        Map<String, String> source = new LinkedHashMap<String, String>();
        source.put(WellKnownCharacterLogMetadata.CLASS_LEVEL, "2");
        CharacterLogEvent event = valid().metadata(source).build();

        source.put(WellKnownCharacterLogMetadata.CLASS_INDEX, "0");

        assertEquals(1, event.getMetadata().size());
    }

    @Test
    void toBuilder_shouldRoundTrip() {
        CharacterLogEvent event = valid().metadata(
                        Collections.singletonMap(WellKnownCharacterLogMetadata.CLASS_ID, "89"))
                .build();

        assertEquals(event, event.toBuilder().build());
        assertEquals(event.hashCode(), event.toBuilder().build().hashCode());
    }

    @Test
    void equals_shouldDistinguishType() {
        CharacterLogEvent.Builder builder = valid();

        assertNotEquals(
                builder.build(),
                builder.type(WellKnownCharacterLogTypes.THIRD_CLASS).build());
    }
}
