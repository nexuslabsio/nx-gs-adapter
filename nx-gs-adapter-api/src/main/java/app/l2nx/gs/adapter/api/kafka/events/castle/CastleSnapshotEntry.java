package app.l2nx.gs.adapter.api.kafka.events.castle;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One castle inside a {@link CastleSnapshotEvent}. Describes the castle's current
 * owning clan and the moment of its next scheduled siege.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getCastleId() castleId} — REQUIRED. Stable per-castle key the
 *   consumer upserts on.</li>
 *   <li>{@link #getName() name} — optional display name. Unlike raid bosses
 *   (resolved from an NPC catalog), the platform has no castle catalog, so the
 *   host carries the name on the wire (typically {@code Castle.getName(...)}).</li>
 *   <li>{@link #getOwnerClanId() ownerClanId} — optional owning clan id. The host
 *   translates its no-owner sentinel (typically {@code 0}) to {@code null}.</li>
 *   <li>{@link #getNextSiegeAt() nextSiegeAt} — optional. Absolute Instant of the
 *   next scheduled siege; {@code null} when unknown / unscheduled. The platform
 *   counts down locally, so the snapshot cadence can be slow.</li>
 *   <li>{@link #getMetadata() metadata} — optional open string→string map of
 *   build-agnostic per-castle attributes. {@code null} when absent; hosts MAY add
 *   arbitrary keys without an API release and consumers ignore unknown keys.</li>
 * </ul>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
 */
public final class CastleSnapshotEntry {

    private final int castleId;
    private final @Nullable String name;
    private final @Nullable Long ownerClanId;
    private final @Nullable Instant nextSiegeAt;
    private final @Nullable Map<String, String> metadata;

    public CastleSnapshotEntry(int castleId,
                               @Nullable String name,
                               @Nullable Long ownerClanId,
                               @Nullable Instant nextSiegeAt,
                               @Nullable Map<String, String> metadata) {
        this.castleId = castleId;
        this.name = name;
        this.ownerClanId = ownerClanId;
        this.nextSiegeAt = nextSiegeAt;
        this.metadata = metadata == null
                ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Stable per-castle key the platform upserts on inside a snapshot.
     */
    public int getCastleId() {
        return castleId;
    }

    public @Nullable String getName() {
        return name;
    }

    /**
     * Owning clan id, or {@code null} when the castle is unowned (host maps its
     * own no-owner sentinel to {@code null}).
     */
    public @Nullable Long getOwnerClanId() {
        return ownerClanId;
    }

    /**
     * Absolute Instant of the next scheduled siege, or {@code null} when unknown
     * or unscheduled.
     */
    public @Nullable Instant getNextSiegeAt() {
        return nextSiegeAt;
    }

    /**
     * Open string→string map of build-agnostic per-castle attributes, or
     * {@code null} when absent. When non-null the returned map is unmodifiable.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .castleId(castleId)
                .name(name)
                .ownerClanId(ownerClanId)
                .nextSiegeAt(nextSiegeAt)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CastleSnapshotEntry)) return false;
        CastleSnapshotEntry that = (CastleSnapshotEntry) o;
        return castleId == that.castleId
                && Objects.equals(name, that.name)
                && Objects.equals(ownerClanId, that.ownerClanId)
                && Objects.equals(nextSiegeAt, that.nextSiegeAt)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(castleId, name, ownerClanId, nextSiegeAt, metadata);
    }

    @Override
    public String toString() {
        return "CastleSnapshotEntry[castleId=" + castleId
                + ", name=" + name
                + ", ownerClanId=" + ownerClanId
                + ", nextSiegeAt=" + nextSiegeAt
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private int castleId;
        private @Nullable String name;
        private @Nullable Long ownerClanId;
        private @Nullable Instant nextSiegeAt;
        private @Nullable Map<String, String> metadata;

        public Builder castleId(int castleId) {
            this.castleId = castleId;
            return this;
        }

        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder ownerClanId(@Nullable Long ownerClanId) {
            this.ownerClanId = ownerClanId;
            return this;
        }

        public Builder nextSiegeAt(@Nullable Instant nextSiegeAt) {
            this.nextSiegeAt = nextSiegeAt;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public CastleSnapshotEntry build() {
            return new CastleSnapshotEntry(castleId, name, ownerClanId, nextSiegeAt, metadata);
        }
    }
}
