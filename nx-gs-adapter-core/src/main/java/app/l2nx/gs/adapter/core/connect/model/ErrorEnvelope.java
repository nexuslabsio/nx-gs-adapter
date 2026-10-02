package app.l2nx.gs.adapter.core.connect.model;

/** Either field may be {@code null} when the body doesn't match the envelope (e.g. 5xx HTML page, empty body). */
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
