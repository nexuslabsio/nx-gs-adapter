package app.l2nx.gs.adapter.api.kafka.commands.announcement;

import java.util.Objects;

/**
 * Success payload of {@link AnnounceNowCommand}. Carries optional telemetry
 * about the broadcast; the platform does not depend on this value for
 * correctness, only for observability (command-audit detail rendering).
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class AnnounceResult {

    private final int linesSent;

    public AnnounceResult(int linesSent) {
        this.linesSent = linesSent;
    }

    /**
     * Number of physical chat lines actually broadcast — typically the count
     * of non-empty lines after the host splits
     * {@link AnnounceNowCommand#getText()} on {@code \n}. Best-effort
     * telemetry; hosts that do not track this MAY report {@code 0}.
     */
    public int getLinesSent() {
        return linesSent;
    }

    public Builder toBuilder() {
        return new Builder().linesSent(linesSent);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnnounceResult)) return false;
        AnnounceResult that = (AnnounceResult) o;
        return linesSent == that.linesSent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(linesSent);
    }

    @Override
    public String toString() {
        return "AnnounceResult[linesSent=" + linesSent + "]";
    }

    public static final class Builder {
        private int linesSent;

        public Builder linesSent(int linesSent) {
            this.linesSent = linesSent;
            return this;
        }

        public AnnounceResult build() {
            return new AnnounceResult(linesSent);
        }
    }
}
