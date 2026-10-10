package app.l2nx.gs.adapter.api.kafka.events.push;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ItemDepletedEventTest {

    private static final UUID EVENT_ID = UUID.fromString("018f5fa3-1e3d-7000-8000-000000000000");

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(
                NullPointerException.class,
                () -> ItemDepletedEvent.builder()
                        .characterId(268483345L)
                        .itemTemplateId(1062)
                        .build());
    }

    @Test
    void toBuilder_shouldCopyEveryField() {
        ItemDepletedEvent expected = ItemDepletedEvent.builder()
                .eventId(EVENT_ID)
                .characterId(268483345L)
                .itemTemplateId(1062)
                .build();

        ItemDepletedEvent actual = expected.toBuilder().build();

        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
    }

    @Test
    void equals_shouldDiffer_whenItemTemplateIdDiffers() {
        ItemDepletedEvent event = ItemDepletedEvent.builder()
                .eventId(EVENT_ID)
                .characterId(268483345L)
                .itemTemplateId(1062)
                .build();

        assertNotEquals(event, event.toBuilder().itemTemplateId(1086).build());
    }

    @Test
    void json_shouldCarryTheContractFieldNames() {
        ItemDepletedEvent event = ItemDepletedEvent.builder()
                .eventId(EVENT_ID)
                .characterId(268483345L)
                .itemTemplateId(1062)
                .build();

        JsonObject actual = new Gson().toJsonTree(event).getAsJsonObject();

        assertEquals(EVENT_ID.toString(), actual.get("eventId").getAsString());
        assertEquals(268483345L, actual.get("characterId").getAsLong());
        assertEquals(1062, actual.get("itemTemplateId").getAsInt());
        assertEquals(3, actual.size());
    }
}
