package app.l2nx.gs.adapter.api.kafka.commands;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommandResultTest {

    @Test
    void ok_shouldHaveStatusOkNoPayloadNoProblem() {
        CommandResult<Void> r = CommandResult.ok();

        assertTrue(r.isOk());
        assertEquals(CommandStatus.OK, r.getStatus());
        assertEquals(CommandStatus.Tier.OK, r.getTier());
        assertNull(r.getPayload());
        assertNull(r.getProblem());
    }

    @Test
    void ok_withPayload_shouldCarryPayload() {
        CommandResult<String> r = CommandResult.ok("hello");

        assertTrue(r.isOk());
        assertEquals("hello", r.getPayload());
        assertNull(r.getProblem());
    }

    @Test
    void error_shouldFlipOkAndCarryStatusAndProblemTitle() {
        CommandResult<Void> r = CommandResult.error(CommandStatus.NOT_FOUND, "Character not found");

        assertFalse(r.isOk());
        assertEquals(CommandStatus.NOT_FOUND, r.getStatus());
        assertEquals(CommandStatus.Tier.CLIENT_ERROR, r.getTier());
        assertEquals("Character not found", r.getProblem().getTitle());
        assertNull(r.getPayload());
    }

    @Test
    void error_withSingleExtension_shouldExposeIt() {
        CommandResult<Void> r = CommandResult.error(
                CommandStatus.NOT_FOUND, "not found", "charId", 12345L);

        assertEquals(CommandStatus.NOT_FOUND, r.getStatus());
        assertEquals(12345L, r.getProblem().getExtensions().get("charId"));
    }

    @Test
    void error_withPrebuiltProblem_shouldExposeProblem() {
        CommandProblem problem = CommandProblem.builder()
                .title("Mail attachment validation failed")
                .detail("3 of 5 items rejected")
                .extension("first.reason", "unknown template id")
                .build();
        CommandResult<Void> r = CommandResult.error(CommandStatus.VALIDATION_FAILED, problem);

        assertFalse(r.isOk());
        assertEquals(CommandStatus.VALIDATION_FAILED, r.getStatus());
        assertSame(problem, r.getProblem());
    }

    @Test
    void notFound_factory_shouldShortcutToNotFoundStatus() {
        CommandResult<Void> r = CommandResult.notFound("Character not found", "charId", 12345L);

        assertEquals(CommandStatus.NOT_FOUND, r.getStatus());
        assertEquals("Character not found", r.getProblem().getTitle());
        assertEquals(12345L, r.getProblem().getExtensions().get("charId"));
    }

    @Test
    void invalidState_factory_shouldShortcutToInvalidStateStatus() {
        CommandResult<Void> r = CommandResult.invalidState("Capacity exceeded");

        assertEquals(CommandStatus.INVALID_STATE, r.getStatus());
        assertEquals("Capacity exceeded", r.getProblem().getTitle());
    }

    @Test
    void validationFailed_withField_shouldEmitFieldExtension() {
        CommandResult<Void> r = CommandResult.validationFailed("count must be positive", "count");

        assertEquals(CommandStatus.VALIDATION_FAILED, r.getStatus());
        assertEquals("count", r.getProblem().getExtensions().get("field"));
    }

    @Test
    void constructor_shouldRejectOkWithProblem() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<Void>(CommandStatus.OK, null, CommandProblem.of("oops")));
    }

    @Test
    void constructor_shouldRejectNonOkWithoutProblem() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<Void>(CommandStatus.NOT_FOUND, null, null));
    }

    @Test
    void constructor_shouldRejectNonOkWithPayload() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<String>(CommandStatus.NOT_FOUND, "payload", CommandProblem.of("nope")));
    }

    @Test
    void constructor_shouldRejectNullStatus() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<Void>(null, null, null));
    }

    @Test
    void equals_shouldDistinguishOnStatus() {
        CommandResult<Void> a = CommandResult.notFound("not found");
        CommandResult<Void> b = CommandResult.forbidden("not found");

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnPayload() {
        CommandResult<String> a = CommandResult.ok("x");
        CommandResult<String> b = CommandResult.ok("y");

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldMatchSameOkAndPayload() {
        assertEquals(CommandResult.ok("x"), CommandResult.ok("x"));
    }

    @Test
    void hashCode_shouldMatchEquals() {
        CommandResult<String> a = CommandResult.ok("x");
        CommandResult<String> b = CommandResult.ok("x");

        assertEquals(a.hashCode(), b.hashCode());
    }
}
