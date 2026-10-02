package app.l2nx.gs.adapter.core;

/** Terminal states ({@link #FAILED}, {@link #REJECTED}, {@link #CLOSED}) never re-enter the connect / heartbeat loop. */
public enum AdapterState {
    INIT,

    REGISTERING,

    ACTIVE,

    /** Transient failure (5xx / 409 / network), retry scheduled. */
    DEGRADED,

    /** Terminal: config error or 401 invalid server-key. */
    FAILED,

    /** Terminal: 403 GAME_SERVER_DEACTIVATED. */
    REJECTED,

    DISABLED,

    CLOSED
}
