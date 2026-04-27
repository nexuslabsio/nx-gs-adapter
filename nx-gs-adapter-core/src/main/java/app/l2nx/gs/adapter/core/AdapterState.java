package app.l2nx.gs.adapter.core;

/**
 * Lifecycle states observable via {@link NxAdapter#state()} / {@link NxAdapter#onStateChange}.
 *
 * <p>Transitions are atomic; observers see each state at most once per logical
 * transition. Terminal states ({@link #FAILED}, {@link #REJECTED}, {@link #CLOSED})
 * never re-enter the connect / heartbeat loop.</p>
 */
public enum AdapterState {

    /**
     * Initial state before {@link NxAdapter#start()} runs.
     */
    INIT,

    /**
     * Connect attempt in flight (first attempt, or any retry).
     */
    REGISTERING,

    /**
     * Connected, heartbeat running.
     */
    ACTIVE,

    /**
     * Transient failure (5xx / 409 / network) — retry scheduled.
     */
    DEGRADED,

    /**
     * Terminal failure due to config error or 401 (invalid server-key).
     */
    FAILED,

    /**
     * Terminal — server deactivated by tenant (403 GAME_SERVER_DEACTIVATED).
     */
    REJECTED,

    /**
     * Adapter is loaded but {@code l2nx.enabled=false} — no work performed.
     */
    DISABLED,

    /**
     * Terminal — JVM shutdown hook fired or {@link NxAdapter#shutdown()} called.
     */
    CLOSED
}
