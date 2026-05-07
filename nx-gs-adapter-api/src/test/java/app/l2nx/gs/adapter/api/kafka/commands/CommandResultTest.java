package app.l2nx.gs.adapter.api.kafka.commands;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandResultTest {

    @Test
    void success_shouldHaveNoErrorCodeAndNoPayload() {
        CommandResult<Void> r = CommandResult.success();

        assertTrue(r.isSuccess());
        assertNull(r.getErrorCode());
        assertTrue(r.getErrorDetails().isEmpty());
        assertNull(r.getPayload());
    }

    @Test
    void success_withPayload_shouldCarryPayload() {
        CommandResult<String> r = CommandResult.success("hello");

        assertTrue(r.isSuccess());
        assertEquals("hello", r.getPayload());
    }

    @Test
    void error_shouldFlipSuccessAndCarryCode() {
        CommandResult<Void> r = CommandResult.error(ErrorCode.NOT_FOUND);

        assertFalse(r.isSuccess());
        assertEquals(ErrorCode.NOT_FOUND, r.getErrorCode());
        assertTrue(r.getErrorDetails().isEmpty());
    }

    @Test
    void error_withSingleDetail_shouldExposeIt() {
        CommandResult<Void> r = CommandResult.error(ErrorCode.NOT_FOUND, "charId", "12345");

        assertEquals(ErrorCode.NOT_FOUND, r.getErrorCode());
        assertEquals("12345", r.getErrorDetails().get("charId"));
    }

    @Test
    void builder_multiDetail_shouldChainErrorDetail() {
        CommandResult<Void> r = CommandResult.<Void>builder()
                .errorCode(ErrorCode.VALIDATION_FAILED)
                .errorDetail("field", "amount")
                .errorDetail("got", "-100")
                .build();

        assertFalse(r.isSuccess());
        assertEquals(ErrorCode.VALIDATION_FAILED, r.getErrorCode());
        assertEquals("amount", r.getErrorDetails().get("field"));
        assertEquals("-100", r.getErrorDetails().get("got"));
    }

    @Test
    void builder_withoutErrorCode_shouldProduceSuccess() {
        CommandResult<String> r = CommandResult.<String>builder()
                .payload("ok")
                .build();

        assertTrue(r.isSuccess());
        assertEquals("ok", r.getPayload());
    }

    @Test
    void constructor_shouldRejectIncoherentSuccessWithErrorCode() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<Void>(true, ErrorCode.NOT_FOUND, null, null));
    }

    @Test
    void constructor_shouldRejectFailureWithoutErrorCode() {
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult<Void>(false, null, null, null));
    }

    @Test
    void getErrorDetails_shouldNormalizeNullToEmptyMap() {
        CommandResult<Void> r = new CommandResult<Void>(true, null, null, null);

        assertNotNull(r.getErrorDetails());
        assertTrue(r.getErrorDetails().isEmpty());
    }

    @Test
    void getErrorDetails_shouldBeUnmodifiable() {
        CommandResult<Void> r = CommandResult.error(ErrorCode.NOT_FOUND, "k", "v");

        assertThrows(UnsupportedOperationException.class,
                () -> r.getErrorDetails().put("x", "y"));
    }

    @Test
    void toBuilder_shouldRoundtripSuccess() {
        CommandResult<String> original = CommandResult.success("payload");

        CommandResult<String> copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void toBuilder_shouldRoundtripError() {
        Map<String, String> details = new LinkedHashMap<String, String>();
        details.put("k1", "v1");
        details.put("k2", "v2");
        CommandResult<Void> original = new CommandResult<Void>(false, ErrorCode.FORBIDDEN, details, null);

        CommandResult<Void> copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void toBuilder_thenAddDetail_shouldNotMutateOriginal() {
        CommandResult<Void> original = CommandResult.error(ErrorCode.NOT_FOUND, "a", "1");

        CommandResult<Void> mutated = original.toBuilder()
                .errorDetail("b", "2")
                .build();

        assertEquals(1, original.getErrorDetails().size());
        assertEquals(2, mutated.getErrorDetails().size());
    }

    @Test
    void equals_shouldDistinguishOnErrorCode() {
        CommandResult<Void> a = CommandResult.error(ErrorCode.NOT_FOUND);
        CommandResult<Void> b = CommandResult.error(ErrorCode.FORBIDDEN);

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishOnPayload() {
        CommandResult<String> a = CommandResult.success("x");
        CommandResult<String> b = CommandResult.success("y");

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldMatchSameSuccessAndPayload() {
        assertEquals(CommandResult.success("x"), CommandResult.success("x"));
    }
}
