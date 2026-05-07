package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.rest.SyncTopics;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

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
 * {@code <tenant>.gs.events.<family>} topics. Future phases will extend with
 * operator-config access and an inbound-commands capability.</p>
 *
 * <p>{@link #events()} is excluded from {@link #equals(Object)} / {@link #hashCode()} /
 * {@link #toString()} — it is a service handle, not part of the value-typed
 * identity bundle. Two contexts with the same identity bits compare equal
 * regardless of which {@link NxEvents} implementation they wrap.</p>
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

    public ConnectContext(UUID tenantId,
                          String tenantSlug,
                          UUID serverId,
                          String serverSlug,
                          String serverName,
                          String adapterVersion,
                          @Nullable SyncTopics syncTopics,
                          @Nullable NxEvents events) {
        this.tenantId = tenantId;
        this.tenantSlug = tenantSlug;
        this.serverId = serverId;
        this.serverSlug = serverSlug;
        this.serverName = serverName;
        this.adapterVersion = adapterVersion;
        this.syncTopics = syncTopics == null ? new SyncTopics(null, null, null) : syncTopics;
        this.events = events == null ? NoOpEvents.INSTANCE : events;
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
     * {@code getSyncTopics().getDb()} / {@code .getRuntime()} / {@code .getDp()}
     * and treat empty as {@code DISABLED}.
     */
    public SyncTopics getSyncTopics() {
        return syncTopics;
    }

    /**
     * Per-family discrete-fact fanout capability. Always non-null — a
     * {@code null} passed to the constructor is normalized to a no-op
     * implementation that swallows every publish call (with a DEBUG log entry).
     * Phase-1 host code calls {@code ctx.events().publishPremium(event)};
     * future families add sibling methods to {@link NxEvents}.
     */
    public NxEvents events() {
        return events;
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
                .events(events);
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
        return Objects.hash(tenantId, tenantSlug, serverId, serverSlug, serverName,
                adapterVersion, syncTopics);
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

        public ConnectContext build() {
            return new ConnectContext(tenantId, tenantSlug, serverId, serverSlug,
                    serverName, adapterVersion, syncTopics, events);
        }
    }
}
