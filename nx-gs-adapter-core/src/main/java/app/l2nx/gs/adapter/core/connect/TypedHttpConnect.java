package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectRequest;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Generic single-attempt HTTP POST + JSON-deserialize helper used by the
 * host-type {@link HostConnectFlow} implementations. Parameterized on the
 * response type so the same code path serves both gameserver and login-server
 * handshakes. Stateless; instantiate per call or reuse — no shared mutable
 * state.
 *
 * <p>Transport invariants: 5s connect timeout, 10s read timeout,
 * {@code Connection: close}, 1 MiB response body char cap.</p>
 *
 * @param <R> JSON DTO type Gson deserializes the 200 body into
 */
final class TypedHttpConnect<R> {

    private static final NxLog log = NxLogFactory.getLogger(TypedHttpConnect.class);

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 10_000;
    static final int MAX_RESPONSE_BODY_CHARS = 1 << 20;

    private final Class<R> responseType;
    private final Gson gson;

    TypedHttpConnect(Class<R> responseType) {
        this(responseType, new Gson());
    }

    TypedHttpConnect(Class<R> responseType, Gson gson) {
        this.responseType = responseType;
        this.gson = gson;
    }

    TypedConnectOutcome<R> exchange(String url, String serverKey, ConnectRequest body) {
        HttpURLConnection conn = null;
        try {
            URL endpoint = new URL(url);
            conn = (HttpURLConnection) endpoint.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setRequestProperty("Authorization", "Bearer " + serverKey);
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Connection", "close");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream();
                    Writer w = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
                gson.toJson(body, w);
            }

            int status = conn.getResponseCode();

            if (status == HttpURLConnection.HTTP_OK) {
                String responseBody = readBody(conn.getInputStream());
                try {
                    R parsed = gson.fromJson(responseBody, responseType);
                    if (parsed == null) {
                        return TypedConnectOutcome.ioFailure(new IOException("connect: 200 with empty body"));
                    }
                    return TypedConnectOutcome.ok(parsed);
                } catch (JsonSyntaxException e) {
                    log.warn(
                            "connect: 200 with malformed JSON ({})",
                            e.getClass().getSimpleName());
                    return TypedConnectOutcome.ioFailure(new IOException("connect: malformed response body", e));
                }
            }

            InputStream errStream;
            try {
                errStream = errorStreamOf(conn);
            } catch (IOException e) {
                return TypedConnectOutcome.ioFailure(e);
            }
            String errBody = readBody(errStream);
            ErrorEnvelope envelope = parseEnvelope(errBody);
            return TypedConnectOutcome.httpError(status, envelope);

        } catch (IOException e) {
            return TypedConnectOutcome.ioFailure(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private ErrorEnvelope parseEnvelope(String body) {
        if (body == null || body.isEmpty()) {
            return new ErrorEnvelope(null, null);
        }
        try {
            ErrorEnvelope env = gson.fromJson(body, ErrorEnvelope.class);
            if (env == null || (env.getCode() == null && env.getMessage() == null)) {
                return new ErrorEnvelope(null, body);
            }
            return env;
        } catch (JsonSyntaxException e) {
            return new ErrorEnvelope(null, body);
        }
    }

    private static InputStream errorStreamOf(HttpURLConnection conn) throws IOException {
        InputStream err = conn.getErrorStream();
        if (err != null) {
            return err;
        }
        return conn.getInputStream();
    }

    static String readBody(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        try (Reader r = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            char[] buf = new char[4096];
            StringBuilder sb = new StringBuilder();
            int read;
            while ((read = r.read(buf)) != -1) {
                if (sb.length() + read > MAX_RESPONSE_BODY_CHARS) {
                    throw new IOException("connect: response body exceeds " + MAX_RESPONSE_BODY_CHARS + " chars");
                }
                sb.append(buf, 0, read);
            }
            return sb.toString();
        }
    }
}
