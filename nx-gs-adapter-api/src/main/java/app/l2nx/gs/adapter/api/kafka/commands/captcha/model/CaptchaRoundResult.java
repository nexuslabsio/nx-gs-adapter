package app.l2nx.gs.adapter.api.kafka.commands.captcha.model;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One picture shown to the player during a captcha check.
 */
public final class CaptchaRoundResult {

    private final int index;
    private final String questionType;
    private final @Nullable Integer pickedSlot;
    private final boolean correct;
    private final @Nullable Long answerTimeMs;

    public CaptchaRoundResult(
            int index,
            String questionType,
            @Nullable Integer pickedSlot,
            boolean correct,
            @Nullable Long answerTimeMs) {
        if (questionType == null) {
            throw new IllegalArgumentException("questionType is required");
        }
        this.index = index;
        this.questionType = questionType;
        this.pickedSlot = pickedSlot;
        this.correct = correct;
        this.answerTimeMs = answerTimeMs;
    }

    /** 1-based position in the check. */
    public int getIndex() {
        return index;
    }

    /** Host vocabulary, UPPER_SNAKE; stored verbatim by the platform. */
    public String getQuestionType() {
        return questionType;
    }

    /** 0-based button the player clicked; {@code null} when the round timed out unanswered. */
    public @Nullable Integer getPickedSlot() {
        return pickedSlot;
    }

    /** {@code false} for a timed-out round. */
    public boolean isCorrect() {
        return correct;
    }

    /** Host-measured from sending the picture to the click (excludes Kafka and platform latency); {@code null} when timed out. */
    public @Nullable Long getAnswerTimeMs() {
        return answerTimeMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CaptchaRoundResult)) return false;
        CaptchaRoundResult that = (CaptchaRoundResult) o;
        return index == that.index
                && correct == that.correct
                && questionType.equals(that.questionType)
                && Objects.equals(pickedSlot, that.pickedSlot)
                && Objects.equals(answerTimeMs, that.answerTimeMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, questionType, pickedSlot, correct, answerTimeMs);
    }

    @Override
    public String toString() {
        return "CaptchaRoundResult[index=" + index + ", questionType=" + questionType + ", pickedSlot=" + pickedSlot
                + ", correct=" + correct + ", answerTimeMs=" + answerTimeMs + "]";
    }

    public static final class Builder {
        private int index;
        private String questionType;
        private @Nullable Integer pickedSlot;
        private boolean correct;
        private @Nullable Long answerTimeMs;

        public Builder index(int index) {
            this.index = index;
            return this;
        }

        public Builder questionType(String questionType) {
            this.questionType = questionType;
            return this;
        }

        public Builder pickedSlot(@Nullable Integer pickedSlot) {
            this.pickedSlot = pickedSlot;
            return this;
        }

        public Builder correct(boolean correct) {
            this.correct = correct;
            return this;
        }

        public Builder answerTimeMs(@Nullable Long answerTimeMs) {
            this.answerTimeMs = answerTimeMs;
            return this;
        }

        public CaptchaRoundResult build() {
            return new CaptchaRoundResult(index, questionType, pickedSlot, correct, answerTimeMs);
        }
    }
}
