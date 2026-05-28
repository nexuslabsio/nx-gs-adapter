package app.l2nx.gs.adapter.core.connect;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

/**
 * Outcome of one {@link HostConnectFlow#connect()} attempt. Parameterized on
 * the host-type response DTO so the same shape carries both gameserver and
 * login-server responses.
 *
 * <p>Three shapes:</p>
 * <ul>
 *   <li>{@link #ok(Object)} — HTTP 200 + parsed response body.</li>
 *   <li>{@link #httpError(int, ErrorEnvelope)} — HTTP 4xx / 5xx with
 *   optional typed envelope.</li>
 *   <li>{@link #ioFailure(IOException)} — transport failure or malformed
 *   200 body.</li>
 * </ul>
 *
 * @param <R> typed response DTO (e.g.
 *            {@link app.l2nx.gs.adapter.api.rest.ConnectResponse},
 *            {@link app.l2nx.gs.adapter.api.rest.LoginServerConnectResponse})
 */
public final class TypedConnectOutcome<R> {

    private final int statusCode;
    private final @Nullable R response;
    private final @Nullable ErrorEnvelope error;
    private final @Nullable IOException ioException;

    private TypedConnectOutcome(int statusCode,
                                @Nullable R response,
                                @Nullable ErrorEnvelope error,
                                @Nullable IOException ioException) {
        this.statusCode = statusCode;
        this.response = response;
        this.error = error;
        this.ioException = ioException;
    }

    public static <R> TypedConnectOutcome<R> ok(R response) {
        return new TypedConnectOutcome<R>(HttpURLConnection.HTTP_OK, response, null, null);
    }

    public static <R> TypedConnectOutcome<R> httpError(int statusCode, ErrorEnvelope envelope) {
        return new TypedConnectOutcome<R>(statusCode, null, envelope, null);
    }

    public static <R> TypedConnectOutcome<R> ioFailure(IOException e) {
        return new TypedConnectOutcome<R>(-1, null, null, e);
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Optional<R> getResponse() {
        return Optional.ofNullable(response);
    }

    public Optional<ErrorEnvelope> getError() {
        return Optional.ofNullable(error);
    }

    public Optional<IOException> getIoException() {
        return Optional.ofNullable(ioException);
    }

    public boolean isIoFailure() {
        return ioException != null;
    }
}
