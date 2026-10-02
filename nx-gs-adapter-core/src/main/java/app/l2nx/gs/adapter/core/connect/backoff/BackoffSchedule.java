package app.l2nx.gs.adapter.core.connect.backoff;

import app.l2nx.gs.adapter.core.connect.flow.ConnectFlow;
import java.time.Duration;

/** Stateless; {@link ConnectFlow} owns the attempt counter. */
public interface BackoffSchedule {

    /** @param attempt 1-based retry index; 1 = first retry after the initial failure */
    Duration next(int attempt);
}
