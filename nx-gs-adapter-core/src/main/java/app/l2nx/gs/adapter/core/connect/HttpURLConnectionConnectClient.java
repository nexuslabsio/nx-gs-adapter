package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectRequest;
import app.l2nx.gs.adapter.api.rest.ConnectResponse;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * JDK-only {@link ConnectClient} backed by {@link HttpURLConnection} + Gson.
 *
 * <p>Sets {@code Authorization: Bearer <serverKey>}, {@code Content-Type:
 * application/json; charset=UTF-8}, {@code Connection: close} (avoids leaking
 * connections into a host-JVM connection pool we don't own). Connect timeout
 * 5s, read timeout 10s — adapter is fire-and-forget; long stalls would just
 * delay the inevitable retry.</p>
 */
public final class HttpURLConnectionConnectClient implements ConnectClient {

    private static final NxLog log = NxLogFactory.getLogger(HttpURLConnectionConnectClient.class);

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 10_000;
    /**
     * Hard cap on response body size — guards the host JVM from OOM on a runaway response.
     */
    static final int MAX_RESPONSE_BODY_BYTES = 1 << 20; // 1 MiB

    private final Gson gson = new Gson();

    @Override
    public ConnectResult connect(String url, String serverKey, ConnectRequest body) {
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
                    ConnectResponse parsed = gson.fromJson(responseBody, ConnectResponse.class);
                    if (parsed == null) {
                        return ConnectResult.ioFailure(
                                new IOException("connect: 200 with empty body"));
                    }
                    return ConnectResult.success(parsed);
                } catch (JsonSyntaxException e) {
                    log.warn("connect: 200 with malformed JSON ({})", e.getClass().getSimpleName());
                    return ConnectResult.ioFailure(
                            new IOException("connect: malformed response body", e));
                }
            }

            InputStream errStream;
            try {
                errStream = errorStreamOf(conn);
            } catch (IOException e) {
                // Couldn't even read the error body — surface as transient IO failure
                // so ConnectFlow retries instead of misclassifying as terminal.
                return ConnectResult.ioFailure(e);
            }
            String errBody = readBody(errStream);
            ErrorEnvelope envelope = parseEnvelope(errBody);
            return ConnectResult.httpError(status, envelope);

        } catch (IOException e) {
            return ConnectResult.ioFailure(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static InputStream errorStreamOf(HttpURLConnection conn) throws IOException {
        InputStream err = conn.getErrorStream();
        if (err != null) {
            return err;
        }
        return conn.getInputStream();
    }

    /**
     * Reads the response body as UTF-8 with a hard size cap. Reads bytes (not lines)
     * so the original payload is preserved verbatim — parsing CRLF / preserving
     * quoted strings is the JSON parser's job, not ours.
     */
    static String readBody(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        try (Reader r = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            char[] buf = new char[4096];
            StringBuilder sb = new StringBuilder();
            int read;
            while ((read = r.read(buf)) != -1) {
                if (sb.length() + read > MAX_RESPONSE_BODY_BYTES) {
                    throw new IOException("connect: response body exceeds " + MAX_RESPONSE_BODY_BYTES + " bytes");
                }
                sb.append(buf, 0, read);
            }
            return sb.toString();
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
}
