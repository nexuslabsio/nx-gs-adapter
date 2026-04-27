package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectRequest;
import app.l2nx.gs.adapter.core.config.AdapterConfig;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Drives the {@code POST /api/tenants/servers/connect} lifecycle and emits an
 * {@link Outcome} per logical state transition. Status-code dispatch:
 * <ul>
 *   <li>{@code 200} → {@link Outcome#ACTIVE}</li>
 *   <li>{@code 401} → {@link Outcome#FAILED} (terminal, no retry)</li>
 *   <li>{@code 403} + {@code code=GAME_SERVER_DEACTIVATED} →
 *       {@link Outcome#REJECTED} (terminal, no retry)</li>
 *   <li>{@code 409} + {@code code=KAFKA_CREDENTIALS_MISSING} → {@link Outcome#TRANSIENT}
 *       (retry via {@link BackoffSchedule})</li>
 *   <li>{@code 5xx} / {@link java.io.IOException} → {@link Outcome#TRANSIENT}
 *       (retry via {@link BackoffSchedule})</li>
 *   <li>any other status → {@link Outcome#FAILED} (treated as terminal so we don't
 *       hammer the platform on an unexpected response shape)</li>
 * </ul>
 *
 * <p>{@link Outcome#STARTING} is emitted at the top of every run (initial submit
 * and every retry) so the orchestrator can drive the {@code REGISTERING} transition.</p>
 */
public final class ConnectFlow implements Runnable {

    private static final NxLog log = NxLogFactory.getLogger(ConnectFlow.class);

    private static final String CONNECT_PATH = "/api/tenants/servers/connect";
    private static final String CODE_GAME_SERVER_DEACTIVATED = "GAME_SERVER_DEACTIVATED";
    private static final String CODE_KAFKA_CREDENTIALS_MISSING = "KAFKA_CREDENTIALS_MISSING";

    /**
     * Strips bearer tokens from any text routed through {@link NxLog}.
     */
    private static final Pattern BEARER_PATTERN = Pattern.compile("Bearer\\s+\\S+");

    private final AdapterConfig config;
    private final ConnectClient client;
    private final BackoffSchedule backoff;
    private final ScheduledExecutorService scheduler;
    private final Consumer<Outcome> onOutcome;

    private final AtomicInteger attempt = new AtomicInteger(0);

    public ConnectFlow(AdapterConfig config,
                       ConnectClient client,
                       BackoffSchedule backoff,
                       ScheduledExecutorService scheduler,
                       Consumer<Outcome> onOutcome) {
        this.config = config;
        this.client = client;
        this.backoff = backoff;
        this.scheduler = scheduler;
        this.onOutcome = onOutcome;
    }

    @Override
    public void run() {
        emit(Outcome.STARTING);
        try {
            String url = buildUrl(config.getPlatformUrl());
            ConnectRequest body = ConnectRequest.builder()
                    .adapterVersion(config.getAdapterVersion())
                    .build();
            ConnectResult result = client.connect(url, config.getServerKey(), body);
            dispatch(result);
        } catch (Throwable t) {
            // Defensive: ConnectClient is contracted not to throw, but a faulty impl
            // (or a downstream wiring bug) must not bring down the daemon thread.
            // Log only the exception class — message may carry the bearer token if
            // the JDK threw IllegalArgumentException from setRequestProperty.
            log.error("Connect attempt threw {}", t.getClass().getName());
            emit(Outcome.TRANSIENT);
            scheduleRetry();
        }
    }

    private void dispatch(ConnectResult result) {
        if (result.isIoFailure()) {
            String msg = sanitize(result.getIoException().map(Throwable::getMessage).orElse(null));
            log.warn("Connect IO failure: {} — retrying with backoff", msg);
            emit(Outcome.TRANSIENT);
            scheduleRetry();
            return;
        }

        int status = result.getStatusCode();
        if (status == HttpURLConnection.HTTP_OK) {
            log.info("Connect succeeded — adapter ACTIVE");
            attempt.set(0);
            emit(Outcome.ACTIVE);
            return;
        }
        if (status == HttpURLConnection.HTTP_UNAUTHORIZED) {
            log.error("Connect rejected with 401 — server-key invalid (terminal)");
            emit(Outcome.FAILED);
            return;
        }
        if (status == HttpURLConnection.HTTP_FORBIDDEN
                && hasCode(result, CODE_GAME_SERVER_DEACTIVATED)) {
            log.error("Connect rejected with 403 GAME_SERVER_DEACTIVATED (terminal)");
            emit(Outcome.REJECTED);
            return;
        }
        if (status == HttpURLConnection.HTTP_CONFLICT
                && hasCode(result, CODE_KAFKA_CREDENTIALS_MISSING)) {
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
        int n = attempt.incrementAndGet();
        Duration delay = backoff.next(n);
        try {
            scheduler.schedule(this, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // RejectedExecutionException on a shutdown scheduler — log class only.
            log.error("Failed to schedule connect retry attempt {}: {}", n, t.getClass().getName());
        }
    }

    private static boolean hasCode(ConnectResult result, String code) {
        return result.getError().map(e -> code.equals(e.getCode())).orElse(false);
    }

    /**
     * {@link app.l2nx.gs.adapter.core.config.ConfigResolver} normalizes {@code platformUrl}
     * to a https URL with no trailing slash, query, or fragment, so the connect URL is
     * just the base + path. Defensive trailing-slash strip is kept for tests that
     * bypass the resolver via fixtures.
     */
    static String buildUrl(String platformUrl) {
        String base = platformUrl.endsWith("/")
                ? platformUrl.substring(0, platformUrl.length() - 1)
                : platformUrl;
        return base + CONNECT_PATH;
    }

    static String sanitize(String text) {
        if (text == null) {
            return "(no message)";
        }
        return BEARER_PATTERN.matcher(text).replaceAll("Bearer ***");
    }

    /**
     * Coarse-grained connect-flow events surfaced to the orchestrator
     * ({@link app.l2nx.gs.adapter.core.NxAdapter}).
     */
    public enum Outcome {
        /**
         * A connect attempt is about to be executed (initial submit or retry).
         */
        STARTING,
        /**
         * 200 — adapter is connected.
         */
        ACTIVE,
        /**
         * Transient failure — retry scheduled.
         */
        TRANSIENT,
        /**
         * Terminal non-recoverable failure — no further retries.
         */
        FAILED,
        /**
         * Terminal — server deactivated by tenant.
         */
        REJECTED
    }
}
