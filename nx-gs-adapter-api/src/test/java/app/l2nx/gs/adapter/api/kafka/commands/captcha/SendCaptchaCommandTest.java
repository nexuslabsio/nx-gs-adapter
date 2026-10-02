package app.l2nx.gs.adapter.api.kafka.commands.captcha;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SendCaptchaCommandTest {

    @Test
    void builder_shouldRoundtripFields() {
        SendCaptchaCommand cmd = SendCaptchaCommand.builder()
                .characterId(42L)
                .issuedBy("sac-sentinel")
                .staffNotes("episode 7")
                .build();

        assertEquals(42L, cmd.getCharacterId().longValue());
        assertEquals("sac-sentinel", cmd.getIssuedBy());
        assertEquals("episode 7", cmd.getStaffNotes());
    }

    @Test
    void constructor_shouldRejectNullCharacterId() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new SendCaptchaCommand(null, null, null));
        assertTrue(ex.getMessage().contains("characterId"));
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SendCaptchaCommand original =
                SendCaptchaCommand.builder().characterId(1L).issuedBy("gm").build();

        assertEquals(original, original.toBuilder().build());
        assertEquals(original.hashCode(), original.toBuilder().build().hashCode());
    }
}
