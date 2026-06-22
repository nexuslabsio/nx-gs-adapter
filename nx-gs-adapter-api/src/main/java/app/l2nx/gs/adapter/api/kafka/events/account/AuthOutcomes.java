package app.l2nx.gs.adapter.api.kafka.events.account;

/**
 * Non-binding catalog of well-known {@link AccountAuthAttemptEvent#getOutcome()}
 * values produced by hosted login servers. Consumers MUST treat unknown
 * outcomes as valid (free-form on the wire) — adding a constant here is
 * documentation, not a runtime gate.
 */
public final class AuthOutcomes {

    public static final String SUCCESS = "SUCCESS";
    public static final String WRONG_PASSWORD = "WRONG_PASSWORD";
    public static final String ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND";
    public static final String BANNED = "BANNED";
    public static final String IP_RESTRICTED = "IP_RESTRICTED";
    public static final String ACCESS_DENIED = "ACCESS_DENIED";
    public static final String PASSWORD_EXPIRED = "PASSWORD_EXPIRED";
    public static final String RATE_LIMITED_BY_IP = "RATE_LIMITED_BY_IP";

    private AuthOutcomes() {}
}
