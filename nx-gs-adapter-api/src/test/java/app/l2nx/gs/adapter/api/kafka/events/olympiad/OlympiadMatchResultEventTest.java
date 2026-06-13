package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OlympiadMatchResultEventTest {

    @Test
    void getClanId_shouldBeNullable() {
        OlympiadMatchResultEvent event = newBuilder()
                .clanId(null)
                .opponentClanId(null)
                .build();

        assertNull(event.getClanId());
        assertNull(event.getOpponentClanId());
    }

    @Test
    void getFightStartedAt_shouldBeNullable_signalingNoFightOccurred() {
        OlympiadMatchResultEvent event = newBuilder()
                .reason(OlympiadMatchReason.BOTH_DEFAULTED)
                .result(OlympiadMatchResult.DRAW)
                .fightStartedAt(null)
                .fightDurationSec(0L)
                .damageDealt(0)
                .opponentDamageDealt(0)
                .build();

        assertNull(event.getFightStartedAt());
        assertEquals(0L, event.getFightDurationSec());
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        OlympiadMatchResultEvent original = OlympiadMatchResultEvent.builder()
                .eventId(UUID.randomUUID())
                .matchId(UUID.randomUUID())
                .olympiadCycle(7)
                .gameType(OlympiadGameType.CLASSED)
                .charId(268437521L).classId(88).clazz(CharacterClass.DUELIST).clanId(101L)
                .opponentCharId(268437522L).opponentClassId(92).opponentClazz(CharacterClass.SAGITTARIUS)
                .opponentClanId(102L)
                .result(OlympiadMatchResult.WIN)
                .reason(OlympiadMatchReason.NORMAL)
                .pointsBefore(40).pointsAfter(43)
                .damageDealt(12_500).opponentDamageDealt(9_400)
                .fightStartedAt(Instant.parse("2026-06-01T12:00:00Z"))
                .fightDurationSec(178L)
                .build();

        OlympiadMatchResultEvent copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertEquals(CharacterClass.DUELIST, copy.getClazz());
        assertEquals(CharacterClass.SAGITTARIUS, copy.getOpponentClazz());
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishClazz() {
        assertNotEquals(
                newBuilder().clazz(CharacterClass.DUELIST).build(),
                newBuilder().clazz(CharacterClass.ADVENTURER).build());
    }

    @Test
    void getClazz_shouldBeNull_whenLegacyNumericOnly() {
        OlympiadMatchResultEvent event = newBuilder().build();
        assertNull(event.getClazz());
        assertNull(event.getOpponentClazz());
    }

    @Test
    void equals_shouldDistinguishResult() {
        OlympiadMatchResultEvent.Builder base = newBuilder();
        OlympiadMatchResultEvent win = base.result(OlympiadMatchResult.WIN).build();
        OlympiadMatchResultEvent loss = base.result(OlympiadMatchResult.LOSS).build();

        assertNotEquals(win, loss);
    }

    @Test
    void equals_shouldDistinguishReason() {
        OlympiadMatchResultEvent.Builder base = newBuilder();
        OlympiadMatchResultEvent normal = base.reason(OlympiadMatchReason.NORMAL).build();
        OlympiadMatchResultEvent dc = base.reason(OlympiadMatchReason.OPPONENT_DISCONNECTED).build();

        assertNotEquals(normal, dc);
    }

    @Test
    void equals_shouldDistinguishCycle() {
        OlympiadMatchResultEvent a = newBuilder().olympiadCycle(7).build();
        OlympiadMatchResultEvent b = newBuilder().olympiadCycle(8).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishPointsAfter() {
        OlympiadMatchResultEvent a = newBuilder().pointsBefore(40).pointsAfter(43).build();
        OlympiadMatchResultEvent b = newBuilder().pointsBefore(40).pointsAfter(44).build();

        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldDistinguishMatchId() {
        UUID eventId = UUID.randomUUID();
        OlympiadMatchResultEvent a = newBuilder().eventId(eventId).matchId(UUID.randomUUID()).build();
        OlympiadMatchResultEvent b = newBuilder().eventId(eventId).matchId(UUID.randomUUID()).build();

        assertNotEquals(a, b);
    }

    @Test
    void toString_shouldRenderCharIdAndOutcome() {
        OlympiadMatchResultEvent event = newBuilder()
                .charId(268437521L)
                .result(OlympiadMatchResult.WIN)
                .reason(OlympiadMatchReason.OPPONENT_DEFAULTED)
                .build();

        String s = event.toString();
        assertTrue(s.contains("charId=268437521"));
        assertTrue(s.contains("result=WIN"));
        assertTrue(s.contains("reason=OPPONENT_DEFAULTED"));
    }

    private static OlympiadMatchResultEvent.Builder newBuilder() {
        return OlympiadMatchResultEvent.builder()
                .eventId(UUID.randomUUID())
                .matchId(UUID.randomUUID())
                .olympiadCycle(7)
                .gameType(OlympiadGameType.CLASSED)
                .charId(268437521L).classId(88).clanId(101L)
                .opponentCharId(268437522L).opponentClassId(92).opponentClanId(102L)
                .result(OlympiadMatchResult.WIN)
                .reason(OlympiadMatchReason.NORMAL)
                .pointsBefore(40).pointsAfter(43)
                .damageDealt(12_500).opponentDamageDealt(9_400)
                .fightStartedAt(Instant.parse("2026-06-01T12:00:00Z"))
                .fightDurationSec(178L);
    }
}
