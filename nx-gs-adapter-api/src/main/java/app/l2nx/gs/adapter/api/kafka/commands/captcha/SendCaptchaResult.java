package app.l2nx.gs.adapter.api.kafka.commands.captcha;

import app.l2nx.gs.adapter.api.kafka.commands.captcha.model.CaptchaRoundResult;
import app.l2nx.gs.adapter.api.kafka.commands.captcha.model.WellKnownCaptchaOutcomes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Final outcome of a captcha check, produced once when the check ends — never a progress update.
 */
public final class SendCaptchaResult {

    private final Long characterId;
    private final @Nullable String issuedBy;
    private final String outcome;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final long durationMs;
    private final List<CaptchaRoundResult> rounds;
    private final Map<String, String> metadata;

    public SendCaptchaResult(
            Long characterId,
            @Nullable String issuedBy,
            String outcome,
            Instant startedAt,
            Instant finishedAt,
            long durationMs,
            @Nullable List<CaptchaRoundResult> rounds,
            @Nullable Map<String, String> metadata) {
        if (characterId == null || outcome == null || startedAt == null || finishedAt == null) {
            throw new IllegalArgumentException("characterId, outcome, startedAt and finishedAt are required");
        }
        this.characterId = characterId;
        this.issuedBy = issuedBy;
        this.outcome = outcome;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.durationMs = durationMs;
        this.rounds = rounds == null || rounds.isEmpty()
                ? Collections.<CaptchaRoundResult>emptyList()
                : Collections.unmodifiableList(new ArrayList<CaptchaRoundResult>(rounds));
        this.metadata = metadata == null || metadata.isEmpty()
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public Long getCharacterId() {
        return characterId;
    }

    public @Nullable String getIssuedBy() {
        return issuedBy;
    }

    /** Open string, canonical values in {@link WellKnownCaptchaOutcomes}. */
    public String getOutcome() {
        return outcome;
    }

    /** Host clock, UTC. */
    public Instant getStartedAt() {
        return startedAt;
    }

    /** Host clock, UTC. */
    public Instant getFinishedAt() {
        return finishedAt;
    }

    public long getDurationMs() {
        return durationMs;
    }

    /** Every picture shown, in order; empty when the check ended before the first one. */
    public List<CaptchaRoundResult> getRounds() {
        return rounds;
    }

    /**
     * What the host did about the result, host-defined and with no stable key set yet. Ban-like
     * consequences use the platform ban vocabulary ({@code ban.type}, {@code ban.expiresAt}); a
     * disconnect is {@code kick=true}. Never null.
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SendCaptchaResult)) return false;
        SendCaptchaResult that = (SendCaptchaResult) o;
        return durationMs == that.durationMs
                && characterId.equals(that.characterId)
                && Objects.equals(issuedBy, that.issuedBy)
                && outcome.equals(that.outcome)
                && startedAt.equals(that.startedAt)
                && finishedAt.equals(that.finishedAt)
                && rounds.equals(that.rounds)
                && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(characterId, issuedBy, outcome, startedAt, finishedAt, durationMs, rounds, metadata);
    }

    @Override
    public String toString() {
        return "SendCaptchaResult[characterId=" + characterId + ", issuedBy=" + issuedBy + ", outcome=" + outcome
                + ", startedAt=" + startedAt + ", finishedAt=" + finishedAt + ", durationMs=" + durationMs
                + ", rounds=" + rounds + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private Long characterId;
        private @Nullable String issuedBy;
        private String outcome;
        private Instant startedAt;
        private Instant finishedAt;
        private long durationMs;
        private @Nullable List<CaptchaRoundResult> rounds;
        private @Nullable Map<String, String> metadata;

        public Builder characterId(Long characterId) {
            this.characterId = characterId;
            return this;
        }

        public Builder issuedBy(@Nullable String issuedBy) {
            this.issuedBy = issuedBy;
            return this;
        }

        public Builder outcome(String outcome) {
            this.outcome = outcome;
            return this;
        }

        public Builder startedAt(Instant startedAt) {
            this.startedAt = startedAt;
            return this;
        }

        public Builder finishedAt(Instant finishedAt) {
            this.finishedAt = finishedAt;
            return this;
        }

        public Builder durationMs(long durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        public Builder rounds(@Nullable List<CaptchaRoundResult> rounds) {
            this.rounds = rounds;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public SendCaptchaResult build() {
            return new SendCaptchaResult(
                    characterId, issuedBy, outcome, startedAt, finishedAt, durationMs, rounds, metadata);
        }
    }
}
