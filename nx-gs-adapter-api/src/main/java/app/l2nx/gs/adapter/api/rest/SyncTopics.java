package app.l2nx.gs.adapter.api.rest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Per-namespace entity-to-topic maps returned in {@link ConnectResponse}.
 * <ul>
 *     <li>{@link #getDb()} - {@code db-sync}: {@code <tenant>.gs.sync.db.<entity>}.</li>
 *     <li>{@link #getRuntime()} - {@code runtime-sync}: {@code <tenant>.gs.sync.runtime.<entity>}.</li>
 *     <li>{@link #getGd()} - {@code gd-sync} (datapack templates): {@code <tenant>.gd.sync.<entity>}.</li>
 * </ul>
 * The same entity name may appear in several namespaces. Maps are copied defensively and exposed unmodifiable; {@code null} is normalized to empty, which both mean {@code DISABLED} for that module.
 */
public final class SyncTopics {

    private final Map<String, String> db;
    private final Map<String, String> runtime;
    private final Map<String, String> gd;

    public SyncTopics(
            @Nullable Map<String, String> db, @Nullable Map<String, String> runtime, @Nullable Map<String, String> gd) {
        this.db = freeze(db);
        this.runtime = freeze(runtime);
        this.gd = freeze(gd);
    }

    /** Never null; getter normalizes a missing namespace (Gson bypasses the constructor) to empty. */
    public Map<String, String> getDb() {
        return db == null ? Collections.emptyMap() : db;
    }

    public Map<String, String> getRuntime() {
        return runtime == null ? Collections.emptyMap() : runtime;
    }

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
        return Objects.equals(db, that.db) && Objects.equals(runtime, that.runtime) && Objects.equals(gd, that.gd);
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
