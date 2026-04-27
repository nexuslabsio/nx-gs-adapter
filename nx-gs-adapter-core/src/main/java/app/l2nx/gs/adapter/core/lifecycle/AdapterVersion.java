package app.l2nx.gs.adapter.core.lifecycle;

/**
 * Resolves the adapter version from the JAR manifest's {@code Implementation-Version}.
 * Lives in {@code lifecycle} (not {@code config}) so the banner can call it before the
 * config resolver runs — even a bad {@code -Dl2nx.config-file} path still gets a banner.
 */
public final class AdapterVersion {

    private static final String FALLBACK = "0.0.0-unknown";

    private AdapterVersion() {
    }

    public static String resolve() {
        String version = AdapterVersion.class.getPackage().getImplementationVersion();
        return version != null ? version : FALLBACK;
    }
}
