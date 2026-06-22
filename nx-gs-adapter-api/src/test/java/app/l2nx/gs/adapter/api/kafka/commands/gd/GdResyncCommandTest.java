package app.l2nx.gs.adapter.api.kafka.commands.gd;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class GdResyncCommandTest {

    @Test
    void command_shouldBeValueEqual_regardlessOfInstance() {
        assertEquals(new GdResyncCommand(), GdResyncCommand.builder().build());
        assertEquals(new GdResyncCommand().hashCode(), new GdResyncCommand().hashCode());
    }

    @Test
    void result_shouldFreezeAcceptedEntities() {
        GdResyncResult result = GdResyncResult.builder()
                .acceptedEntities(Arrays.asList("itemtemplate", "instance"))
                .build();

        assertEquals(Arrays.asList("itemtemplate", "instance"), result.getAcceptedEntities());
        assertThrows(
                UnsupportedOperationException.class,
                () -> result.getAcceptedEntities().add("npctemplate"));
    }

    @Test
    void result_shouldNormalizeNullToEmpty() {
        GdResyncResult result = new GdResyncResult(null);

        assertTrue(result.getAcceptedEntities().isEmpty());
    }

    @Test
    void result_shouldRoundtripThroughToBuilder() {
        GdResyncResult original = new GdResyncResult(Collections.singletonList("skill"));

        GdResyncResult copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertNotSame(original, copy);
    }
}
