package app.l2nx.gs.adapter.core.config;

/**
 * Immutable holder for adapter configuration resolved at startup.
 * Built by {@link ConfigResolver#resolve()}; not constructed directly.
 */
public final class AdapterConfig {

    private final String serverKey;
    private final String platformUrl;
    private final String adapterVersion;
    private final boolean enabled;

    AdapterConfig(String serverKey, String platformUrl, String adapterVersion, boolean enabled) {
        this.serverKey = serverKey;
        this.platformUrl = platformUrl;
        this.adapterVersion = adapterVersion;
        this.enabled = enabled;
    }

    public String getServerKey() {
        return serverKey;
    }

    public String getPlatformUrl() {
        return platformUrl;
    }

    public String getAdapterVersion() {
        return adapterVersion;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
