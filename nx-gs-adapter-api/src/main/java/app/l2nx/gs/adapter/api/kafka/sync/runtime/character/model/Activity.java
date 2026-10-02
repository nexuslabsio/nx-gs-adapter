package app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model;

import app.l2nx.gs.adapter.api.kafka.sync.runtime.character.CharacterRuntimeDto;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One entry of {@link CharacterRuntimeDto#getActivities()}: a required open {@code type} (canonical values in
 * {@link WellKnownActivities}) plus optional open {@code String->String} metadata (keys in {@link WellKnownActivityMetadata}).
 * Everything beyond {@code type} is deliberately untyped so no core's activities leak into the contract.
 */
public final class Activity {

    private final String type;
    private final @Nullable Map<String, String> metadata;

    public Activity(String type, @Nullable Map<String, String> metadata) {
        this.type = type;
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public String getType() {
        return type;
    }

    /**
     * Null when absent; otherwise unmodifiable and insertion-ordered.
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
