package app.l2nx.gs.adapter.core.connect;

import java.time.Duration;

/**
 * Stateless retry-delay generator. Implementations return a {@link Duration} based
 * on the attempt number; {@link ConnectFlow} owns the attempt counter.
 */
public interface BackoffSchedule {

    /**
     * @param attempt 1-based retry index (attempt {@code 1} = first retry after the
     *                initial failure)
     * @return the delay before the next retry attempt
     */
    Duration next(int attempt);
}
