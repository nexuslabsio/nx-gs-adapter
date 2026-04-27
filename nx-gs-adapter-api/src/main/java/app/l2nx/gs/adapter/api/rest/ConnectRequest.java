package app.l2nx.gs.adapter.api.rest;

import java.util.Objects;

/**
 * Adapter handshake request body.
 *
 * <p>Wire JSON shape: <code>{"adapterVersion": "..."}</code></p>
 *
 * @see ConnectResponse
 */
public final class ConnectRequest {

    private final String adapterVersion;

    /**
     * All-args constructor used by JSON binders: Spring/Jackson via parameter-name binding
     * (requires {@code -parameters} compile flag, configured in this module's build), Gson
     * via field reflection.
     */
    public ConnectRequest(String adapterVersion) {
        this.adapterVersion = adapterVersion;
    }

    /**
     * Adapter version as reported by the JAR manifest, or operator override.
     */
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

    /**
     * Hand-written builder — mirrors the {@code @Builder(toBuilder = true)} ergonomics of records.
     */
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
