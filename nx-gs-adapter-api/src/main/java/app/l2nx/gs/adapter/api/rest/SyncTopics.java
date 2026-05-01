package app.l2nx.gs.adapter.api.rest;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-namespace per-entity Kafka topic addressing returned in {@link ConnectResponse}.
 *
 * <p>Three coexisting namespaces:</p>
 * <ul>
 *     <li>{@link #getDb()} — DB-derived sync via {@code db-sync} module
 *     ({@code <tenant>.gs.sync.db.<entity>}). Keys: {@code "clan"},
 *     {@code "character"}, {@code "item"}, …</li>
 *     <li>{@link #getRuntime()} — in-memory runtime sync via
 *     {@code runtime-sync} module ({@code <tenant>.gs.sync.runtime.<entity>}).
 *     Keys: {@code "character"}, …</li>
 *     <li>{@link #getDp()} — datapack-derived sync via future {@code dp-sync} modules
 *     ({@code <tenant>.gs.sync.dp.<entity>}). Reserved for follow-up slices.</li>
 * </ul>
 *
 * <p>Per-namespace shape: {@code Map<entityName, fullyQualifiedTopic>}. Same entity
 * name MAY appear in more than one namespace (e.g. {@code character} in {@code db}
 * AND {@code runtime}) — namespace separation here is what disambiguates them on
 * the wire.</p>
 *
 * <p>Each namespace map is defensively copied on construction and exposed as
 * unmodifiable. {@code null} on any namespace is normalized to an empty map at
 * this layer — modules treat {@code null} and empty as identical (both drive
 * {@code DISABLED} for the corresponding sync module), and erasing the
 * distinction here keeps engine code branch-free on namespace presence.</p>
 */
public final class SyncTopics {

    private final Map<String, String> db;
    private final Map<String, String> runtime;
    private final Map<String, String> dp;

    public SyncTopics(@Nullable Map<String, String> db,
                      @Nullable Map<String, String> runtime,
                      @Nullable Map<String, String> dp) {
        this.db = freeze(db);
        this.runtime = freeze(runtime);
        this.dp = freeze(dp);
    }

    /**
     * DB-derived per-entity topics ({@code db-sync} module). Always non-null;
     * empty map means no DB sync entities are configured. Getter normalizes
     * {@code null} to an empty map so JSON deserialization (which bypasses the
     * ctor and may leave the field null when the namespace was absent on the
     * wire) does not break the contract.
     */
    public Map<String, String> getDb() {
        return db == null ? Collections.emptyMap() : db;
    }

    /**
     * In-memory runtime per-entity topics ({@code runtime-sync} module). Always
     * non-null; empty map means no runtime sync entities are configured.
     */
    public Map<String, String> getRuntime() {
        return runtime == null ? Collections.emptyMap() : runtime;
    }

    /**
     * Datapack-derived per-entity topics (future {@code dp-sync} modules). Always
     * non-null; empty map means no datapack sync entities are configured.
     */
    public Map<String, String> getDp() {
        return dp == null ? Collections.emptyMap() : dp;
    }

    public Builder toBuilder() {
        return new Builder().db(db).runtime(runtime).dp(dp);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, String> freeze(@Nullable Map<String, String> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncTopics)) return false;
        SyncTopics that = (SyncTopics) o;
        return Objects.equals(db, that.db)
                && Objects.equals(runtime, that.runtime)
                && Objects.equals(dp, that.dp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, runtime, dp);
    }

    @Override
    public String toString() {
        return "SyncTopics[db=" + db + ", runtime=" + runtime + ", dp=" + dp + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, String> db;
        private @Nullable Map<String, String> runtime;
        private @Nullable Map<String, String> dp;

        public Builder db(@Nullable Map<String, String> db) {
            this.db = db;
            return this;
        }

        public Builder runtime(@Nullable Map<String, String> runtime) {
            this.runtime = runtime;
            return this;
        }

        public Builder dp(@Nullable Map<String, String> dp) {
            this.dp = dp;
            return this;
        }

        public SyncTopics build() {
            return new SyncTopics(db, runtime, dp);
        }
    }
}
