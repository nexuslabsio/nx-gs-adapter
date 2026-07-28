package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One entry of {@link CharacterRuntimeDto#getActivities()} — a
 * build-specific, high-level "what the player is occupied with" signal that lives
 * outside the engine AI state machine (e.g. fishing, trading, autofarming).
 *
 * <p>Deliberately a thin, build-agnostic envelope:
 * <ul>
 *   <li>{@link #getType() type} — REQUIRED discriminator. Canonical values in
 *   {@link WellKnownActivities} ({@code fishing} / {@code trade} / …);
 *   open, so a host MAY emit its own activity key without an API release.</li>
 *   <li>{@link #getMetadata() metadata} — optional open {@code String→String}
 *   map of activity-specific extras, mirroring the {@code metadata} maps on the
 *   discrete event DTOs ({@code BossRespawnEntry}, {@code CharacterPresenceEvent},
 *   {@code GameEventEntry}). Canonical keys in
 *   {@link WellKnownActivityMetadata} (e.g. {@code elapsed_seconds},
 *   {@code store_type}); values are stringified (the platform stores the
 *   whole object as JSON and the dashboard parses what it needs). Hosts MAY add
 *   arbitrary keys; consumers ignore keys they do not understand.</li>
 * </ul>
 *
 * <p>The contract intentionally does NOT type any per-activity field (not even
 * {@code elapsed_seconds}) — everything beyond {@code type} is the open
 * {@code metadata} map. This keeps the wire and the platform's JSONB storage
 * agnostic to which core ships which activity. A {@code null} / empty
 * {@code activities} on {@link CharacterRuntimeDto} means "no special
 * activity".</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
 */
public final class Activity {

    private final String type;
    private final @Nullable Map<String, String> metadata;

    public Activity(String type, @Nullable Map<String, String> metadata) {
        this.type = type;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Activity discriminator — canonical values in
     * {@link WellKnownActivities}. REQUIRED.
     */
    public String getType() {
        return type;
    }

    /**
     * Open {@code String→String} map of activity-specific metadata, or
     * {@code null} when absent. Canonical keys in
     * {@link WellKnownActivityMetadata}. When non-null the returned map
     * is unmodifiable and preserves insertion order.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder().type(type).metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Activity)) return false;
        Activity that = (Activity) o;
        return Objects.equals(type, that.type) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, metadata);
    }

    @Override
    public String toString() {
        return "Activity[type=" + type + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable String type;
        private @Nullable Map<String, String> metadata;

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Activity build() {
            return new Activity(type, metadata);
        }
    }
}
