package app.l2nx.gs.adapter.core.config;

/**
 * Test-only factory exposing the package-private {@link AdapterConfig} constructor
 * to tests in sibling packages. Lives in {@code src/test/java} so it is not
 * published.
 */
public final class AdapterConfigFixtures {

    public static final String VALID_SERVER_KEY = "nx_sk_abcdefghijklmnopqrstuvwxyz012345";
    public static final String DEFAULT_VERSION = "0.0.0-test";

    private AdapterConfigFixtures() {
    }

    public static AdapterConfig enabled(String platformUrl) {
        return new AdapterConfig(VALID_SERVER_KEY, platformUrl, DEFAULT_VERSION, true);
    }

    public static AdapterConfig disabled(String platformUrl) {
        return new AdapterConfig(VALID_SERVER_KEY, platformUrl, DEFAULT_VERSION, false);
    }
}
