package app.l2nx.gs.adapter.api.spi.capability;

import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException;
import java.util.function.Supplier;

/**
 * Hops {@link CommandContext#host()} work onto the host's game-side thread pool (a pool, not a single loop thread), preserving the
 * host's lock ordering. Mutating game state requires a hop; read-only handlers may skip it.
 * Registered via {@code NxAdapter.hostExecutor(Executor)} before {@code start()}; without one, every call
 * throws {@link IllegalStateException}.
 *
 * <p>{@code sync} awaits at most {@code l2nx.commands.host-sync-timeout-ms} (default 30000), then throws
 * {@link HostExecutorTimeoutException}; the bound keeps a saturated pool from wedging the consumer thread.</p>
 */
public interface HostExecutor {

    /** Blocks until done or timeout; task exceptions and {@code Error} propagate to the caller. */
    void sync(Runnable task);

    /** As {@link #sync(Runnable)}, returning the task's value. */
    <T> T sync(Supplier<T> task);

    /** Fire-and-forget; a {@code Throwable} from the task is logged, never seen by the caller. */
    void async(Runnable task);
}
