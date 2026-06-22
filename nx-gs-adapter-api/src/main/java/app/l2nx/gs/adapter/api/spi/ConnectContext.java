package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.rest.SyncTopics;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.jspecify.annotations.Nullable;

/**
 * Identity bundle handed to every {@link AdapterModule#onConnect(ConnectContext)} call
 * after a successful platform handshake. Modules cache only the bits they need;
 * the context itself is immutable.
 *
 * <p>Phase 1 carried identity only. Phase 2 added {@link #getSyncTopics()} —
 * namespaced per-entity Kafka topic names delivered by the platform via
 * {@code ConnectResponse.syncTopics}; consumed by sync modules
 * ({@code db-sync}, {@code runtime-sync}). Phase 3 added {@link #events()} —
 * the {@link NxEvents} capability for per-family discrete-fact fanout to
 * {@code <tenant>.gs.events.<family>} topics. Phase 4 adds {@link #commands()} —
 * the {@link NxCommands} capability for registering inbound command handlers
 * dispatched off the {@code <tenant>.gs.commands} topic. Phase 5 adds
 * {@link #io()} — an adapter-owned bounded {@link Executor} for module /
 * handler-side blocking IO (JDBC, HTTP).</p>
 *
 * <p>{@link #events()}, {@link #commands()}, and {@link #io()} are excluded
 * from {@link #equals(Object)} / {@link #hashCode()} / {@link #toString()} —
 * they are service handles, not part of the value-typed identity bundle. Two
 * contexts with the same identity bits compare equal regardless of which
 * implementations they wrap.</p>
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
        // Direct-run fallback keeps ctx.io().execute(r) usable in tests / pre-wired contexts;
        // production adapter-core injects a bounded pool.
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

    /**
     * Namespaced per-entity Kafka topic addressing for sync modules. Always
     * non-null — a {@code null} {@code ConnectResponse.syncTopics} on the wire
     * is normalized here to an empty {@link SyncTopics} (every namespace
     * resolves to an empty map). Modules read their namespace via
     * {@code getSyncTopics().getDb()} / {@code .getRuntime()} / {@code .getGd()}
     * and treat empty as {@code DISABLED}.
     */
    public SyncTopics getSyncTopics() {
        return syncTopics;
    }

    /**
     * Per-family discrete-fact fanout capability. Always non-null — a
     * {@code null} passed to the constructor is normalized to a no-op
     * implementation that swallows every publish call (with a DEBUG log entry).
     * Host code calls {@code ctx.events().publish(event)}; the runtime type
     * of {@code event} routes to the correct family via the adapter-core
     * type registry. Adding a new event type is a one-line registration —
     * no SPI change.
     */
    public NxEvents events() {
        return events;
    }

    /**
     * Inbound command-handler registration capability. Always non-null — a
     * {@code null} passed to the constructor is normalized to a no-op
     * implementation that drops registrations silently. Host code calls
     * {@code ctx.commands().on(KickCommand.class, handler)} from its
     * {@code onConnect} callback; the adapter dispatches inbound records
     * to the registered handler by {@code Nx-Message-Type} header lookup.
     */
    public NxCommands commands() {
        return commands;
    }

    /**
     * Adapter-owned IO executor. Use for blocking IO (JDBC, HTTP) issued from
     * module / handler-side code. NOT the game-thread executor — modules
     * needing game-state reads/writes go through {@link CommandContext#host()}
     * on a per-invocation basis. Backed by a small bounded pool sized by
     * {@code l2nx.io.workers} (default =
     * {@code max(2, Runtime.getRuntime().availableProcessors() / 2)}); a
     * {@code null} passed to the constructor falls back to a direct-run
     * executor so calls remain safe in tests / pre-wired contexts.
     */
    public Executor io() {
        return io;
    }

    /**
     * Out-of-band sync request capability. Modules with sync responsibilities
     * register triggers via {@link NxSync#registerTrigger(String, NxSyncTrigger)}
     * during {@code onConnect}; host code calls
     * {@code ctx.sync().requestNow(entity, pk)} to demand an immediate sync
     * pass for a specific entity instance. Always non-null — a {@code null}
     * passed to the constructor is normalized to a no-op implementation that
     * silently drops requests.
     */
    public NxSync sync() {
        return sync;
    }

    /**
     * Game-data sync capability. Modules / host hooks call
     * {@code ctx.gameData().publishSnapshot()} to trigger a fresh full snapshot of
     * static game-data templates onto the {@code gd} stream (e.g. after a datapack
     * reload). Always non-null — a {@code null} passed to the constructor is
     * normalized to a no-op that drops the request.
     */
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
