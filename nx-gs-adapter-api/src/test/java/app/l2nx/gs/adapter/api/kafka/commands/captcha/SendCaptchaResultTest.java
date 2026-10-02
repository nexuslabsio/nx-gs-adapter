package app.l2nx.gs.adapter.api.kafka.commands.captcha;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.commands.captcha.model.CaptchaRoundResult;
import app.l2nx.gs.adapter.api.kafka.commands.captcha.model.WellKnownCaptchaOutcomes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SendCaptchaResultTest {

    private static final Instant STARTED = Instant.parse("2026-10-02T14:03:11Z");
    private static final Instant FINISHED = Instant.parse("2026-10-02T14:05:41Z");

    private static SendCaptchaResult.Builder valid() {
        return SendCaptchaResult.builder()
                .characterId(7L)
                .outcome(WellKnownCaptchaOutcomes.PASSED)
                .startedAt(STARTED)
                .finishedAt(FINISHED)
                .durationMs(150_000L);
    }

    @Test
    void constructor_shouldRejectMissingOutcome() {
        assertThrows(IllegalArgumentException.class, () -> valid().outcome(null).build());
    }

    @Test
    void constructor_shouldRejectMissingTimestamps() {
        assertThrows(
                IllegalArgumentException.class, () -> valid().startedAt(null).build());
        assertThrows(
                IllegalArgumentException.class, () -> valid().finishedAt(null).build());
    }

    @Test
    void build_shouldDefaultRoundsAndMetadataToEmpty_whenNull() {
        SendCaptchaResult result = valid().build();

        assertTrue(result.getRounds().isEmpty());
        assertTrue(result.getMetadata().isEmpty());
    }

    @Test
    void build_shouldCopyRoundsAndMetadata() {
        List<CaptchaRoundResult> rounds = new ArrayList<>();
        rounds.add(CaptchaRoundResult.builder()
                .index(1)
                .questionType("ODD_COLOR")
                .pickedSlot(3)
                .correct(true)
                .answerTimeMs(6210L)
                .build());
        Map<String, String> metadata = new HashMap<>();
        metadata.put("kick", "true");

        SendCaptchaResult result = valid().rounds(rounds).metadata(metadata).build();
        rounds.clear();
        metadata.clear();

        assertEquals(1, result.getRounds().size());
        assertEquals("true", result.getMetadata().get("kick"));
        assertThrows(
                UnsupportedOperationException.class, () -> result.getRounds().clear());
        assertThrows(
                UnsupportedOperationException.class, () -> result.getMetadata().put("x", "y"));
    }

    @Test
    void equals_shouldDistinguishOnRounds() {
        SendCaptchaResult a = valid().build();
        SendCaptchaResult b = valid().rounds(Collections.singletonList(CaptchaRoundResult.builder()
                        .index(1)
                        .questionType("SHAPE_COUNT")
                        .build()))
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void roundResult_shouldRejectMissingQuestionType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CaptchaRoundResult.builder().index(1).build());
    }
}
