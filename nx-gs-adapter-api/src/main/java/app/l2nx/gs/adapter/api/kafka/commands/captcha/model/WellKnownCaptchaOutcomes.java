package app.l2nx.gs.adapter.api.kafka.commands.captcha.model;

/**
 * Canonical values of {@code SendCaptchaResult.outcome}. Open string: a consumer treats an unknown
 * value as {@link #ABORTED}.
 */
public final class WellKnownCaptchaOutcomes {

    private WellKnownCaptchaOutcomes() {}

    /** Answered enough rounds correctly. */
    public static final String PASSED = "PASSED";

    /** Ran out of allowed wrong answers. */
    public static final String FAILED_WRONG = "FAILED_WRONG";

    /** A round or the whole check timed out. */
    public static final String FAILED_TIMEOUT = "FAILED_TIMEOUT";

    /** The player left the game or lost the connection while the check was open. */
    public static final String LOGOUT = "LOGOUT";

    /** The host could not run the check to a verdict (no picture, shutdown, released by the host). */
    public static final String ABORTED = "ABORTED";
}
