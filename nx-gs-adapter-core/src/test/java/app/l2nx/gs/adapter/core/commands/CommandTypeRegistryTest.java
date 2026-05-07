package app.l2nx.gs.adapter.core.commands;

import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import app.l2nx.gs.adapter.api.spi.CommandHandler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommandTypeRegistryTest {

    static final class FakeCommandA implements NxCommand<Void> {
    }

    static final class FakeCommandB implements NxCommand<Void> {
    }

    private static final CommandHandler<FakeCommandA, Void> NOOP_HANDLER_A =
            (cmd, ctx) -> CommandResult.success();
    private static final CommandHandler<FakeCommandB, Void> NOOP_HANDLER_B =
            (cmd, ctx) -> CommandResult.success();

    @Test
    void register_firstCall_shouldReturnFalse() {
        CommandTypeRegistry registry = new CommandTypeRegistry();

        boolean overwrote = registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        assertFalse(overwrote);
    }

    @Test
    void register_secondCallSameClass_shouldReturnTrue() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        boolean overwrote = registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        assertTrue(overwrote);
    }

    @Test
    void lookup_byMessageType_shouldReturnBoundClass() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        CommandTypeBinding binding = registry.lookup("FakeCommandA");

        assertNotNull(binding);
        assertEquals(FakeCommandA.class, binding.commandClass());
    }

    @Test
    void lookup_unregisteredType_shouldReturnNull() {
        CommandTypeRegistry registry = new CommandTypeRegistry();

        assertNull(registry.lookup("Whatever"));
    }

    @Test
    void lookup_nullMessageType_shouldReturnNull() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        assertNull(registry.lookup(null));
    }

    @Test
    void snapshotRegisteredTypes_shouldBeSorted() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandB.class, NOOP_HANDLER_B);
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        List<String> snapshot = registry.snapshotRegisteredTypes();

        assertEquals("FakeCommandA", snapshot.get(0));
        assertEquals("FakeCommandB", snapshot.get(1));
    }

    @Test
    void snapshotRegisteredTypes_shouldBeUnmodifiable() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        List<String> snapshot = registry.snapshotRegisteredTypes();

        assertThrows(UnsupportedOperationException.class, () -> snapshot.add("X"));
    }

    @Test
    void snapshotRegisteredTypes_emptyRegistry_shouldReturnEmptyList() {
        CommandTypeRegistry registry = new CommandTypeRegistry();

        assertTrue(registry.snapshotRegisteredTypes().isEmpty());
    }

    @Test
    void binding_shouldExposePreCachedReplyMessageTypeBytes() {
        CommandTypeRegistry registry = new CommandTypeRegistry();
        registry.register(FakeCommandA.class, NOOP_HANDLER_A);

        CommandTypeBinding binding = registry.lookup("FakeCommandA");

        assertNotNull(binding);
        assertArrayEquals("FakeCommandAResult".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                binding.replyMessageTypeBytes());
    }
}
