package app.l2nx.gs.adapter.core.connect;

import app.l2nx.gs.adapter.api.rest.ConnectRequest;

/**
 * Performs the platform-side {@code POST /api/tenants/servers/connect} handshake.
 *
 * <p>Implementations must not throw from {@link #connect}; transport / parsing failures
 * are encoded in the returned {@link ConnectResult}. This keeps {@link ConnectFlow}
 * dispatch logic uniform across success, HTTP errors, and IO failures.</p>
 */
public interface ConnectClient {

    /**
     * Send the connect request and return a typed result.
     *
     * @param url       full URL — typically {@code <platformUrl>/api/tenants/servers/connect}
     * @param serverKey raw server-key value (sent as {@code Authorization: Bearer <key>})
     * @param body      request payload
     * @return outcome of the call — never {@code null}, never throws
     */
    ConnectResult connect(String url, String serverKey, ConnectRequest body);
}
