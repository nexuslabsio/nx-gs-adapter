package app.l2nx.gs.adapter.core.connect;

/**
 * Platform-side error response body — Gson-deserialized from 4xx / 5xx responses.
 *
 * <p>Wire shape: {@code {"code": "...", "message": "..."}}. Either field may be
 * {@code null} when the platform returns a body that doesn't match the envelope
 * (e.g. a generic 5xx HTML page or empty body).</p>
 */
public final class ErrorEnvelope {

    private final String code;
    private final String message;

    public ErrorEnvelope(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
