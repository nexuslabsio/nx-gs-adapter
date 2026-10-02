package app.l2nx.gs.adapter.api.kafka.ops.model;

/**
 * Canonical values for {@link ModuleStatus#getState()}. The wire type is {@code String}, not an enum, so a newer adapter's unknown value still deserializes on an older consumer.
 * Consumers SHOULD treat unknown values as degraded.
 */
public final class ModuleStates {

    private ModuleStates() {}

    public static final String INIT = "INIT";

    public static final String ACTIVE = "ACTIVE";

    public static final String DEGRADED = "DEGRADED";

    public static final String DISABLED = "DISABLED";

    public static final String FAILED = "FAILED";
}
