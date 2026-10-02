package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.capability.NxCommands;
import app.l2nx.gs.adapter.api.spi.capability.NxEvents;
import app.l2nx.gs.adapter.api.spi.capability.NxGameData;
import app.l2nx.gs.adapter.api.spi.capability.NxSync;
import app.l2nx.gs.adapter.api.spi.capability.NxSyncTrigger;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/**
 * Handshake identity plus capability handles passed to {@link AdapterModule#onConnect}.
 * Capability handles ({@code events}, {@code commands}, {@code io}, {@code sync}, {@code gameData}) are
 * excluded from equals/hashCode/toString: contexts with the same identity bits are equal.
 */
public final class ConnectContext {

    private final UUID tenantId;
    private final String tenantSlug;
    private final UUID serverId;
    private final String serverSlug;
    private final String serverName;
    private final String adapterVersion;
    private final SyncTopics syncTopics;
    private final NxEvents events;
    private final NxCommands commands;
    private final Executor io;
    private final NxSync sync;
    private final NxGameData gameData;

    public ConnectContext(
            UUID tenantId,
            String tenantSlug,
            UUID serverId,
            String serverSlug,
            String serverName,
            String adapterVersion,
            @Nullable SyncTopics syncTopics,
            @Nullable NxEvents events,
            @Nullable NxCommands commands,
            @Nullable Executor io,
            @Nullable NxSync sync,
            @Nullable NxGameData gameData) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.syncTopics = syncTopics == null ? new SyncTopics(null, null, null) : syncTopics;
        this.events = events == null ? NoOpEvents.INSTANCE : events;
        this.commands = commands == null ? NoOpCommands.INSTANCE : commands;
        // direct-run fallback; production injects a bounded pool
        this.io = io == null ? DirectExecutor.INSTANCE : io;
        this.sync = sync == null ? NoOpSync.INSTANCE : sync;
        this.gameData = gameData == null ? NoOpGameData.INSTANCE : gameData;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public String getTenantSlug() {
        return tenantSlug;
    }

    public UUID getServerId() {
        return serverId;
    }

    public String getServerSlug() {
        return serverSlug;
    }

    public String getServerName() {
        return serverName;
    }

    public String getAdapterVersion() {
        return adapterVersion;
    }

    /** Never null: an absent wire value becomes empty {@link SyncTopics}; modules treat empty as DISABLED. */
    public SyncTopics getSyncTopics() {
        return syncTopics;
    }

    /** Never null: no-op (publishes dropped) when not wired. The event's runtime type selects the family. */
    public NxEvents events() {
        return events;
    }

    /** Never null: no-op (registrations dropped) when not wired. Dispatch is by {@code Nx-Message-Type}. */
    public NxCommands commands() {
        return commands;
    }

    /**
     * Never null: direct-run executor when not wired, otherwise a bounded pool ({@code l2nx.io.workers}) for blocking IO.
     * Not the game thread; use {@link CommandContext#host()} for that.
     */
    public Executor io() {
        return io;
    }

    /**
     * Never null: no-op when not wired. Register triggers via {@link NxSync#registerTrigger(String, NxSyncTrigger)}
     * during {@code onConnect}.
     */
    public NxSync sync() {
        return sync;
    }

    /** Never null: no-op when not wired. {@code publishSnapshot()} republishes static templates on the {@code gd} stream. */
    public NxGameData gameData() {
        return gameData;
    }

    public Builder toBuilder() {
        return new Builder()
                .tenantId(tenantId)
                .tenantSlug(tenantSlug)
                .serverId(serverId)
                .serverSlug(serverSlug)
                .serverName(serverName)
                .adapterVersion(adapterVersion)
                .syncTopics(syncTopics)
                .events(events)
                .commands(commands)
                .io(io)
                .sync(sync)
                .gameData(gameData);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectContext)) return false;
        ConnectContext that = (ConnectContext) o;
        return Objects.equals(tenantId, that.tenantId)
                && Objects.equals(tenantSlug, that.tenantSlug)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(serverSlug, that.serverSlug)
                && Objects.equals(serverName, that.serverName)
                && Objects.equals(adapterVersion, that.adapterVersion)
                && Objects.equals(syncTopics, that.syncTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName, adapterVersion, syncTopics);
    }

    @Override
    public String toString() {
        return "ConnectContext[tenantId=" + tenantId
                + ", tenantSlug=" + tenantSlug
                + ", serverId=" + serverId
                + ", serverSlug=" + serverSlug
                + ", serverName=" + serverName
                + ", adapterVersion=" + adapterVersion
                + ", syncTopics=" + syncTopics + "]";
    }

    public static final class Builder {
        private UUID tenantId;
        private String tenantSlug;
        private UUID serverId;
        private String serverSlug;
        private String serverName;
        private String adapterVersion;
        private @Nullable SyncTopics syncTopics;
        private @Nullable NxEvents events;
        private @Nullable NxCommands commands;
        private @Nullable Executor io;
        private @Nullable NxSync sync;
        private @Nullable NxGameData gameData;

        public Builder tenantId(UUID tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder tenantSlug(String tenantSlug) {
            this.tenantSlug = tenantSlug;
            return this;
        }

        public Builder serverId(UUID serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder serverSlug(String serverSlug) {
            this.serverSlug = serverSlug;
            return this;
        }

        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public Builder adapterVersion(String adapterVersion) {
            this.adapterVersion = adapterVersion;
            return this;
        }

        public Builder syncTopics(@Nullable SyncTopics syncTopics) {
            this.syncTopics = syncTopics;
            return this;
        }

        public Builder events(@Nullable NxEvents events) {
            this.events = events;
            return this;
        }

        public Builder commands(@Nullable NxCommands commands) {
            this.commands = commands;
            return this;
        }

        public Builder io(@Nullable Executor io) {
            this.io = io;
            return this;
        }

        public Builder sync(@Nullable NxSync sync) {
            this.sync = sync;
            return this;
        }

        public Builder gameData(@Nullable NxGameData gameData) {
            this.gameData = gameData;
            return this;
        }

        public ConnectContext build() {
            return new ConnectContext(
                    tenantId,
                    tenantSlug,
                    serverId,
                    serverSlug,
                    serverName,
                    adapterVersion,
                    syncTopics,
                    events,
                    commands,
                    io,
                    sync,
                    gameData);
        }
    }
}
