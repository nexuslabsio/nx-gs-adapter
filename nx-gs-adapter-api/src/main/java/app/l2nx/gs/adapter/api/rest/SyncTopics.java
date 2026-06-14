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
 *     <li>{@link #getGd()} — game-data (datapack-derived static templates) sync via
 *     the {@code gd-sync} module ({@code <tenant>.gd.sync.<entity>}). Keys:
 *     {@code "itemtemplate"}, {@code "npctemplate"}, {@code "skill"}, …</li>
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
    private final Map<String, String> gd;

    public SyncTopics(@Nullable Map<String, String> db,
                      @Nullable Map<String, String> runtime,
                      @Nullable Map<String, String> gd) {
        this.db = freeze(db);
        this.runtime = freeze(runtime);
        this.gd = freeze(gd);
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
     * Game-data per-entity topics ({@code gd-sync} module). Always non-null;
     * empty map means no game-data sync entities are configured.
     */
    public Map<String, String> getGd() {
        return gd == null ? Collections.emptyMap() : gd;
    }

    public Builder toBuilder() {
        return new Builder().db(db).runtime(runtime).gd(gd);
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
                && Objects.equals(gd, that.gd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, runtime, gd);
    }

    @Override
    public String toString() {
        return "SyncTopics[db=" + db + ", runtime=" + runtime + ", gd=" + gd + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, String> db;
        private @Nullable Map<String, String> runtime;
        private @Nullable Map<String, String> gd;

        public Builder db(@Nullable Map<String, String> db) {
            this.db = db;
            return this;
        }

        public Builder runtime(@Nullable Map<String, String> runtime) {
            this.runtime = runtime;
            return this;
        }

        public Builder gd(@Nullable Map<String, String> gd) {
            this.gd = gd;
            return this;
        }

        public SyncTopics build() {
            return new SyncTopics(db, runtime, gd);
        }
    }
}
