package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectResponse;

import java.io.IOException;
import java.util.Optional;

/**
 * Outcome of a single {@link ConnectClient#connect} invocation.
 *
 * <p>Three shapes:</p>
 * <ul>
 *   <li>{@link #success(ConnectResponse)} — HTTP 200 + parsed response body.</li>
 *   <li>{@link #httpError(int, ErrorEnvelope)} — HTTP 4xx / 5xx (envelope may carry
 *       a typed error {@code code}).</li>
 *   <li>{@link #ioFailure(IOException)} — DNS, connection refused, read timeout,
 *       or malformed JSON in a 200 body.</li>
 * </ul>
 */
public final class ConnectResult {

    private final int statusCode;
    private final ConnectResponse response;
    private final ErrorEnvelope error;
    private final IOException ioException;

    private ConnectResult(int statusCode,
                          ConnectResponse response,
                          ErrorEnvelope error,
                          IOException ioException) {
        this.statusCode = statusCode;
        this.response = response;
        this.error = error;
        this.ioException = ioException;
    }

    public static ConnectResult success(ConnectResponse response) {
        return new ConnectResult(200, response, null, null);
    }

    public static ConnectResult httpError(int statusCode, ErrorEnvelope envelope) {
        return new ConnectResult(statusCode, null, envelope, null);
    }

    public static ConnectResult ioFailure(IOException ioException) {
        return new ConnectResult(-1, null, null, ioException);
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Optional<ConnectResponse> getResponse() {
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
