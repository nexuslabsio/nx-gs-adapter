package app.l2nx.gs.adapter.core.config;

import app.l2nx.gs.adapter.core.commands.CommandsConfig;
import app.l2nx.gs.adapter.core.events.EventsConfig;
import java.util.Collections;

/** Exposes the package-private AdapterConfig constructor to sibling packages. */
public final class AdapterConfigFixtures {

    public static final String VALID_SERVER_KEY = "nx_sk_abcdefghijklmnopqrstuvwxyz012345";
    public static final String DEFAULT_VERSION = "0.0.0-test";

    private AdapterConfigFixtures() {}

    public static AdapterConfig enabled(String platformUrl) {
        return new AdapterConfig(
                VALID_SERVER_KEY,
                platformUrl,
                DEFAULT_VERSION,
                true,
                AdapterConfig.defaultIoWorkers(),
                Collections.emptyMap(),
                EventsConfig.defaults(),
                CommandsConfig.defaults());
    }

    public static AdapterConfig disabled(String platformUrl) {
        return new AdapterConfig(
                VALID_SERVER_KEY,
                platformUrl,
                DEFAULT_VERSION,
                false,
                AdapterConfig.defaultIoWorkers(),
                Collections.emptyMap(),
                EventsConfig.defaults(),
                CommandsConfig.defaults());
    }
}
