package app.l2nx.gs.adapter.api.kafka.events.account;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AccountAuthAttemptEventTest {

    private static final String SAMPLE_EVENT_ID = UUID.randomUUID().toString();
    private static final String SAMPLE_SERVER_ID = UUID.randomUUID().toString();
    private static final Instant SAMPLE_AT = Instant.parse("2026-06-01T12:00:00Z");

    @Test
    void builder_shouldPopulateAllFields() {
        AccountAuthAttemptEvent event = AccountAuthAttemptEvent.builder()
                .eventId(SAMPLE_EVENT_ID)
                .serverId(SAMPLE_SERVER_ID)
                .accountName("olya")
                .clientIp("10.0.0.1")
                .hwid("hw-abc")
                .outcome(AuthOutcomes.WRONG_PASSWORD)
                .attemptedAt(SAMPLE_AT)
                .failureDetail("third try")
                .build();

        assertEquals(SAMPLE_EVENT_ID, event.getEventId());
        assertEquals(SAMPLE_SERVER_ID, event.getServerId());
        assertEquals("olya", event.getAccountName());
        assertEquals("10.0.0.1", event.getClientIp());
        assertEquals("hw-abc", event.getHwid());
        assertEquals("WRONG_PASSWORD", event.getOutcome());
        assertEquals(SAMPLE_AT, event.getAttemptedAt());
        assertEquals("third try", event.getFailureDetail());
    }

    @Test
    void getHwid_shouldBeNullable_forBohptsProtocol() {
        AccountAuthAttemptEvent event = minimal().build();

        assertNull(event.getHwid());
    }

    @Test
    void getFailureDetail_shouldBeNullable_forSuccessfulLogin() {
        AccountAuthAttemptEvent event = minimal()
                .outcome(AuthOutcomes.SUCCESS)
                .build();

        assertNull(event.getFailureDetail());
    }

    @Test
    void constructor_shouldRejectNullEventId() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(null, SAMPLE_SERVER_ID, "a", "1.1.1.1",
                        null, AuthOutcomes.SUCCESS, SAMPLE_AT, null));
    }

    @Test
    void constructor_shouldRejectNullServerId() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(SAMPLE_EVENT_ID, null, "a", "1.1.1.1",
                        null, AuthOutcomes.SUCCESS, SAMPLE_AT, null));
    }

    @Test
    void constructor_shouldRejectNullAccountName() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(SAMPLE_EVENT_ID, SAMPLE_SERVER_ID, null, "1.1.1.1",
                        null, AuthOutcomes.SUCCESS, SAMPLE_AT, null));
    }

    @Test
    void constructor_shouldRejectNullClientIp() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(SAMPLE_EVENT_ID, SAMPLE_SERVER_ID, "a", null,
                        null, AuthOutcomes.SUCCESS, SAMPLE_AT, null));
    }

    @Test
    void constructor_shouldRejectNullOutcome() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(SAMPLE_EVENT_ID, SAMPLE_SERVER_ID, "a", "1.1.1.1",
                        null, null, SAMPLE_AT, null));
    }

    @Test
    void constructor_shouldRejectNullAttemptedAt() {
        assertThrows(NullPointerException.class, () ->
                new AccountAuthAttemptEvent(SAMPLE_EVENT_ID, SAMPLE_SERVER_ID, "a", "1.1.1.1",
                        null, AuthOutcomes.SUCCESS, null, null));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        AccountAuthAttemptEvent original = AccountAuthAttemptEvent.builder()
                .eventId(SAMPLE_EVENT_ID)
                .serverId(SAMPLE_SERVER_ID)
                .accountName("olya")
                .clientIp("10.0.0.1")
                .hwid("hw-abc")
                .outcome(AuthOutcomes.BANNED)
                .attemptedAt(SAMPLE_AT)
                .failureDetail("banned until 2026-12")
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldDistinguishOutcome() {
        AccountAuthAttemptEvent a = minimal().outcome(AuthOutcomes.SUCCESS).build();
        AccountAuthAttemptEvent b = minimal().outcome(AuthOutcomes.WRONG_PASSWORD).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishHwid() {
        AccountAuthAttemptEvent a = minimal().hwid("x").build();
        AccountAuthAttemptEvent b = minimal().hwid("y").build();

        assertNotEquals(a, b);
    }

    @Test
    void toString_shouldRenderAccountAndOutcome() {
        AccountAuthAttemptEvent event = minimal()
                .accountName("alice")
                .outcome(AuthOutcomes.WRONG_PASSWORD)
                .build();

        String s = event.toString();
        assertTrue(s.contains("accountName=alice"));
        assertTrue(s.contains("outcome=WRONG_PASSWORD"));
    }

    @Test
    void getOutcome_shouldAcceptUnknownString() {
        // Consumers MUST handle unknown values gracefully — the wire is free-form.
        AccountAuthAttemptEvent event = minimal().outcome("CORE_VERSION_MISMATCH").build();

        assertEquals("CORE_VERSION_MISMATCH", event.getOutcome());
    }

    private static AccountAuthAttemptEvent.Builder minimal() {
        return AccountAuthAttemptEvent.builder()
                .eventId(SAMPLE_EVENT_ID)
                .serverId(SAMPLE_SERVER_ID)
                .accountName("acc")
                .clientIp("1.1.1.1")
                .outcome(AuthOutcomes.SUCCESS)
                .attemptedAt(SAMPLE_AT);
    }
}
