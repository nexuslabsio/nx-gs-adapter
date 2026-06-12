package app.l2nx.gs.adapter.api.kafka.commands.sync;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ResyncRowsCommandTest {

    private static final UUID RESYNC_ID = UUID.fromString("018f0000-0000-7000-8000-000000000002");

    @Test
    void builder_shouldRoundtripFields() {
        ResyncRowsCommand cmd = ResyncRowsCommand.builder()
                .resyncId(RESYNC_ID)
                .entityName("character")
                .pks(Arrays.asList(1L, 2L, 3L))
                .cascade(true)
                .build();

        assertEquals(RESYNC_ID, cmd.getResyncId());
        assertEquals("character", cmd.getEntityName());
        assertEquals(Arrays.asList(1L, 2L, 3L), cmd.getPks());
        assertTrue(cmd.isCascade());
    }

    @Test
    void builder_shouldDefaultCascadeToFalse() {
        ResyncRowsCommand cmd = ResyncRowsCommand.builder()
                .resyncId(RESYNC_ID)
                .entityName("character")
                .pks(Collections.singletonList(1L))
                .build();

        assertFalse(cmd.isCascade());
    }

    @Test
    void constructor_shouldRejectNullResyncId() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ResyncRowsCommand(null, "character", Collections.singletonList(1L), false));
        assertTrue(ex.getMessage().contains("resyncId"));
    }

    @Test
    void constructor_shouldRejectNullEntityName() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ResyncRowsCommand(RESYNC_ID, null, Collections.singletonList(1L), false));
        assertTrue(ex.getMessage().contains("entityName"));
    }

    @Test
    void constructor_shouldRejectNullPks() {
        assertThrows(IllegalArgumentException.class,
                () -> new ResyncRowsCommand(RESYNC_ID, "character", null, false));
    }

    @Test
    void constructor_shouldRejectEmptyPks() {
        assertThrows(IllegalArgumentException.class,
                () -> new ResyncRowsCommand(RESYNC_ID, "character",
                        Collections.<Long>emptyList(), false));
    }

    @Test
    void constructor_shouldRejectPksOverCap() {
        List<Long> oversized = new ArrayList<Long>(ResyncRowsCommand.MAX_PKS + 1);
        for (long i = 0; i <= ResyncRowsCommand.MAX_PKS; i++) {
            oversized.add(i);
        }

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ResyncRowsCommand(RESYNC_ID, "character", oversized, false));
        assertTrue(ex.getMessage().contains(String.valueOf(ResyncRowsCommand.MAX_PKS)));
    }

    @Test
    void constructor_shouldAcceptExactlyMaxPks() {
        List<Long> atCap = new ArrayList<Long>(ResyncRowsCommand.MAX_PKS);
        for (long i = 0; i < ResyncRowsCommand.MAX_PKS; i++) {
            atCap.add(i);
        }

        assertEquals(ResyncRowsCommand.MAX_PKS,
                new ResyncRowsCommand(RESYNC_ID, "character", atCap, false).getPks().size());
    }

    @Test
    void getPks_shouldBeUnmodifiable() {
        ResyncRowsCommand cmd = new ResyncRowsCommand(RESYNC_ID, "character",
                Collections.singletonList(1L), false);

        assertThrows(UnsupportedOperationException.class, () -> cmd.getPks().add(2L));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ResyncRowsCommand original = ResyncRowsCommand.builder()
                .resyncId(RESYNC_ID)
                .entityName("character")
                .pks(Arrays.asList(7L, 8L))
                .cascade(true)
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishOnCascade() {
        ResyncRowsCommand a = new ResyncRowsCommand(RESYNC_ID, "character",
                Collections.singletonList(1L), true);
        ResyncRowsCommand b = new ResyncRowsCommand(RESYNC_ID, "character",
                Collections.singletonList(1L), false);

        assertNotEquals(a, b);
    }

    @Test
    void implementsNxCommandMarker() {
        assertInstanceOf(NxCommand.class,
                new ResyncRowsCommand(RESYNC_ID, "character", Collections.singletonList(1L), false));
    }
}
