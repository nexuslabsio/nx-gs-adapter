package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;

/**
 * Adapter handshake request, e.g. <code>{"adapterVersion": "..."}</code>.
 *
 * @see ConnectResponse
 */
public final class ConnectRequest {

    private final String adapterVersion;

    /** Spring/Jackson bind by parameter name (needs {@code -parameters}); Gson uses field reflection. */
    public ConnectRequest(String adapterVersion) {
        this.adapterVersion = adapterVersion;
    }

    public String getAdapterVersion() {
        return adapterVersion;
    }

    public Builder toBuilder() {
        return new Builder().adapterVersion(adapterVersion);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectRequest)) return false;
        ConnectRequest that = (ConnectRequest) o;
        return Objects.equals(adapterVersion, that.adapterVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adapterVersion);
    }

    @Override
    public String toString() {
        return "ConnectRequest[adapterVersion=" + adapterVersion + "]";
    }

    public static final class Builder {
        private String adapterVersion;

        public Builder adapterVersion(String adapterVersion) {
            this.adapterVersion = adapterVersion;
            return this;
        }

        public ConnectRequest build() {
            return new ConnectRequest(adapterVersion);
        }
    }
}
