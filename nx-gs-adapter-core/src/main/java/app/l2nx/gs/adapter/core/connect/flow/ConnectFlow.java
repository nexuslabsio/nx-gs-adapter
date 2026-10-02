package app.l2nx.gs.adapter.core.connect.flow;

import app.l2nx.gs.adapter.core.connect.backoff.BackoffSchedule;
import app.l2nx.gs.adapter.core.connect.model.TypedConnectOutcome;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Retry/backoff/state machine over a {@link HostConnectFlow}. 401, 403 GAME_SERVER_DEACTIVATED and any
 * unexpected status are terminal so the platform isn't hammered; 409 KAFKA_CREDENTIALS_MISSING, 5xx and IO retry.
 */
public final class ConnectFlow implements Runnable {

    private static final NxLog log = NxLogFactory.getLogger(ConnectFlow.class);

    private static final String CODE_GAME_SERVER_DEACTIVATED = "GAME_SERVER_DEACTIVATED";
    private static final String CODE_KAFKA_CREDENTIALS_MISSING = "KAFKA_CREDENTIALS_MISSING";

    private static final Pattern BEARER_PATTERN = Pattern.compile("Bearer\\s+\\S+");

    private final HostConnectFlow<?> flow;
    private final BackoffSchedule backoff;
    private final ScheduledExecutorService scheduler;
    private final Consumer<Outcome> onOutcome;
    // Runs before ACTIVE is observable so Kafka is bootstrapped first; null = response not needed.
    private final Consumer<HostConnectFlow<?>> onActiveFlow;

    private final AtomicInteger attempt = new AtomicInteger(0);

    public ConnectFlow(
            HostConnectFlow<?> flow,
            BackoffSchedule backoff,
            ScheduledExecutorService scheduler,
            Consumer<Outcome> onOutcome) {
        this(flow, backoff, scheduler, onOutcome, null);
    }

    public ConnectFlow(
            HostConnectFlow<?> flow,
            BackoffSchedule backoff,
            ScheduledExecutorService scheduler,
            Consumer<Outcome> onOutcome,
            Consumer<HostConnectFlow<?>> onActiveFlow) {
        this.flow = flow;
        this.backoff = backoff;
        this.scheduler = scheduler;
        this.onOutcome = onOutcome;
        this.onActiveFlow = onActiveFlow;
    }

    @Override
    public void run() {
        emit(Outcome.STARTING);
        TypedConnectOutcome<?> outcome;
        try {
            outcome = flow.connect();
        } catch (Throwable t) {
            // Must not kill the daemon thread; class name only - the message may carry the bearer token
            // (IllegalArgumentException from setRequestProperty).
            log.error("Connect attempt threw {}", t.getClass().getName(), t);
            emit(Outcome.TRANSIENT);
            scheduleRetry();
            return;
        }
        dispatch(outcome);
    }

    private void dispatch(TypedConnectOutcome<?> result) {
        if (result.isIoFailure()) {
            String msg =
                    sanitize(result.getIoException().map(Throwable::getMessage).orElse(null));
            log.warn("Connect IO failure: {} — retrying with backoff", msg);
            emit(Outcome.TRANSIENT);
            scheduleRetry();
            return;
        }

        int status = result.getStatusCode();
        if (status == HttpURLConnection.HTTP_OK) {
            log.info("Connect succeeded — platform handshake passed");
            attempt.set(0);
            if (onActiveFlow != null) {
                if (result.getResponse().isPresent()) {
                    try {
                        onActiveFlow.accept(flow);
                    } catch (Throwable t) {
                        log.error("ConnectFlow onActiveFlow threw: {}", t.getMessage(), t);
                    }
                    // Orchestrator owns the post-200 state; emitting ACTIVE would clobber a legitimate DEGRADED.
                    return;
                }
                log.error("HostConnectFlow returned 200 with no parsed body — falling back to bare ACTIVE outcome");
            }
            emit(Outcome.ACTIVE);
            return;
        }
        if (status == HttpURLConnection.HTTP_UNAUTHORIZED) {
            log.error("Connect rejected with 401 — server-key invalid (terminal)");
            emit(Outcome.FAILED);
            return;
        }
        if (status == HttpURLConnection.HTTP_FORBIDDEN && hasCode(result, CODE_GAME_SERVER_DEACTIVATED)) {
            log.error("Connect rejected with 403 GAME_SERVER_DEACTIVATED (terminal)");
            emit(Outcome.REJECTED);
            return;
        }
        if (status == HttpURLConnection.HTTP_CONFLICT && hasCode(result, CODE_KAFKA_CREDENTIALS_MISSING)) {
            log.warn("Connect 409 KAFKA_CREDENTIALS_MISSING — retrying with backoff");
            emit(Outcome.TRANSIENT);
            scheduleRetry();
            return;
        }
        if (status >= 500 && status < 600) {
            log.warn("Connect {} — retrying with backoff", status);
            emit(Outcome.TRANSIENT);
            scheduleRetry();
            return;
        }
        log.error("Connect unexpected status {} — treating as terminal failure", status);
        emit(Outcome.FAILED);
    }

    private void emit(Outcome o) {
        try {
            onOutcome.accept(o);
        } catch (Throwable t) {
            log.error("ConnectFlow outcome consumer threw on {}: {}", o, t.getMessage(), t);
        }
    }

    private void scheduleRetry() {
        // Capped so a long outage can't overflow the counter.
        int n = attempt.updateAndGet(prev -> Math.min(prev + 1, Integer.MAX_VALUE - 1));
        Duration delay = backoff.next(n);
        try {
            scheduler.schedule(this, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // Scheduler shut down; terminal so upstream doesn't wait in REGISTERING/DEGRADED forever.
            log.error(
                    "Failed to schedule connect retry attempt {}: {}",
                    n,
                    t.getClass().getName(),
                    t);
            emit(Outcome.FAILED);
        }
    }

    private static boolean hasCode(TypedConnectOutcome<?> result, String code) {
        return result.getError().map(e -> code.equals(e.getCode())).orElse(false);
    }

    // ConfigResolver already normalizes platformUrl; the slash strip covers fixtures that bypass it.
    static String buildUrl(String platformUrl, String connectPath) {
        String base = platformUrl.endsWith("/") ? platformUrl.substring(0, platformUrl.length() - 1) : platformUrl;
        return base + connectPath;
    }

    static String sanitize(String text) {
        if (text == null) {
            return "(no message)";
        }
        return BEARER_PATTERN.matcher(text).replaceAll("Bearer ***");
    }

    public enum Outcome {
        /** Emitted before every attempt, including retries. */
        STARTING,
        ACTIVE,
        TRANSIENT,
        FAILED,
        REJECTED
    }
}
