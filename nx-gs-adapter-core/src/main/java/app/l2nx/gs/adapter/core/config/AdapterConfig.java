package app.l2nx.gs.adapter.core.config;

import app.l2nx.gs.adapter.core.commands.CommandsConfig;
import app.l2nx.gs.adapter.core.events.EventsConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable holder for adapter configuration resolved at startup.
 * Built by {@link ConfigResolver#resolve()}; not constructed directly.
 */
public final class AdapterConfig {

    /**
     * Floor for the IO worker pool — keeps modules/handlers usable when host
     * JVM reports a single available processor.
     */
    public static final int DEFAULT_IO_WORKERS_MIN = 2;

    /**
     * Default host-type when no {@code l2nx.host-type} is configured —
     * preserves back-compat for pre-host-type adapter deployments.
     */
    public static final String DEFAULT_HOST_TYPE = "gs";

    private final String serverKey;
    private final String platformUrl;
    private final String adapterVersion;
    private final boolean enabled;
    private final int ioWorkers;
    private final Map<String, Object> kafkaProducerOverrides;
    private final EventsConfig events;
    private final CommandsConfig commands;
    private final String hostType;

    AdapterConfig(String serverKey, String platformUrl, String adapterVersion, boolean enabled,
                  int ioWorkers,
                  Map<String, Object> kafkaProducerOverrides, EventsConfig events,
                  CommandsConfig commands) {
        this(serverKey, platformUrl, adapterVersion, enabled, ioWorkers,
                kafkaProducerOverrides, events, commands, DEFAULT_HOST_TYPE);
    }

    AdapterConfig(String serverKey, String platformUrl, String adapterVersion, boolean enabled,
                  int ioWorkers,
                  Map<String, Object> kafkaProducerOverrides, EventsConfig events,
                  CommandsConfig commands, String hostType) {
        this.serverKey = serverKey;
        this.platformUrl = platformUrl;
        this.adapterVersion = adapterVersion;
        this.enabled = enabled;
        this.ioWorkers = ioWorkers;
        this.kafkaProducerOverrides = Collections.unmodifiableMap(new LinkedHashMap<>(kafkaProducerOverrides));
        this.events = events != null ? events : EventsConfig.defaults();
        this.commands = commands != null ? commands : CommandsConfig.defaults();
        this.hostType = hostType != null ? hostType : DEFAULT_HOST_TYPE;
    }

    public static int defaultIoWorkers() {
        return Math.max(DEFAULT_IO_WORKERS_MIN, Runtime.getRuntime().availableProcessors() / 2);
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

    public int getIoWorkers() {
        return ioWorkers;
    }

    public Map<String, Object> getKafkaProducerOverrides() {
        return kafkaProducerOverrides;
    }

    public EventsConfig getEvents() {
        return events;
    }

    public CommandsConfig getCommands() {
        return commands;
    }

    /**
     * Adapter host-type — {@code gs} (game server) or {@code ls} (login
     * server). Selects the platform connect endpoint and the expected
     * server-key property name.
     */
    public String getHostType() {
        return hostType;
    }
}
