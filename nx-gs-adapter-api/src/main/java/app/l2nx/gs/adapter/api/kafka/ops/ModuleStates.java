package app.l2nx.gs.adapter.api.kafka.ops;

/**
 * Canonical values for {@link ModuleStatus#getState()}. The wire type stays
 * {@code String} rather than a JVM enum deliberately — an unrecognised value
 * published by a newer adapter must deserialize cleanly on an older consumer
 * instead of breaking it. Consumers SHOULD treat any value outside this set as
 * degraded rather than failing outright.
 */
public final class ModuleStates {

    private ModuleStates() {}

    public static final String INIT = "INIT";

    public static final String ACTIVE = "ACTIVE";

    public static final String DEGRADED = "DEGRADED";

    public static final String DISABLED = "DISABLED";

    public static final String FAILED = "FAILED";
}
