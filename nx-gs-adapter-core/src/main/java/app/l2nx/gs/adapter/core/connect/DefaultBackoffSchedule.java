package app.l2nx.gs.adapter.core.connect;

import java.time.Duration;

/**
 * Canonical retry schedule: {@code 30s → 1m → 2m → 5m}, capped at 5m for all
 * subsequent attempts.
 */
public final class DefaultBackoffSchedule implements BackoffSchedule {

    private static final Duration[] STEPS = new Duration[]{
            Duration.ofSeconds(30),
            Duration.ofMinutes(1),
            Duration.ofMinutes(2),
            Duration.ofMinutes(5)
    };

    @Override
    public Duration next(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, got " + attempt);
        }
        int idx = Math.min(attempt - 1, STEPS.length - 1);
        return STEPS[idx];
    }
}
