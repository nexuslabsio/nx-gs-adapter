package app.l2nx.gs.adapter.core.connect.model;

import app.l2nx.gs.adapter.core.connect.flow.HostConnectFlow;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Outcome of one {@link HostConnectFlow#connect()}: ok, HTTP error, or IO failure (transport error or malformed 200 body). */
public final class TypedConnectOutcome<R> {

    private final int statusCode;
    private final @Nullable R response;
    private final @Nullable ErrorEnvelope error;
    private final @Nullable IOException ioException;

    private TypedConnectOutcome(
            int statusCode, @Nullable R response, @Nullable ErrorEnvelope error, @Nullable IOException ioException) {
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
