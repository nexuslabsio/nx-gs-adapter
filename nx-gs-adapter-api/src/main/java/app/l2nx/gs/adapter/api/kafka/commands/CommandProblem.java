package app.l2nx.gs.adapter.api.kafka.commands;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Structured problem body for a non-OK {@link CommandResult}. Transport-neutral
 * subset of RFC 9457 — {@code title} stable per problem kind, {@code detail}
 * per-instance context, {@code extensions} free-form Gson-serializable map.
 * HTTP-specific fields (status, type URI, instance URI) live elsewhere
 * ({@link CommandStatus}, {@code Nx-Correlation-Id}).
 */
public final class CommandProblem {

    private final String title;
    private final @Nullable String detail;
    private final Map<String, Object> extensions;

    public CommandProblem(String title, @Nullable String detail, @Nullable Map<String, Object> extensions) {
        if (title == null) {
            throw new IllegalArgumentException("title is required");
        }
        this.title = title;
        this.detail = detail;
        this.extensions = freeze(extensions);
    }

    public String getTitle() {
        return title;
    }

    public @Nullable String getDetail() {
        return detail;
    }

    public Map<String, Object> getExtensions() {
        return extensions;
    }

    public Builder toBuilder() {
        return new Builder().title(title).detail(detail).extensions(extensions);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience: problem with only a title.
     */
    public static CommandProblem of(String title) {
        return new CommandProblem(title, null, null);
    }

    /**
     * Convenience: problem with title + detail.
     */
    public static CommandProblem of(String title, String detail) {
        return new CommandProblem(title, detail, null);
    }

    /**
     * Convenience: problem with title + single-key extension.
     */
    public static CommandProblem of(String title, String extKey, Object extValue) {
        Map<String, Object> ext = new LinkedHashMap<String, Object>();
        ext.put(extKey, extValue);
        return new CommandProblem(title, null, ext);
    }

    private static Map<String, Object> freeze(@Nullable Map<String, Object> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, Object>(src));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommandProblem)) return false;
        CommandProblem that = (CommandProblem) o;
        return Objects.equals(title, that.title)
                && Objects.equals(detail, that.detail)
                && Objects.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, detail, extensions);
    }

    @Override
    public String toString() {
        return "CommandProblem[title=" + title + ", detail=" + detail + ", extensions=" + extensions + "]";
    }

    public static final class Builder {
        private String title;
        private @Nullable String detail;
        private @Nullable Map<String, Object> extensions;

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder detail(@Nullable String detail) {
            this.detail = detail;
            return this;
        }

        public Builder extensions(@Nullable Map<String, Object> extensions) {
            this.extensions = extensions;
            return this;
        }

        public Builder extension(String key, Object value) {
            if (extensions == null) {
                extensions = new LinkedHashMap<String, Object>();
            } else if (!(extensions instanceof LinkedHashMap)) {
                extensions = new LinkedHashMap<String, Object>(extensions);
            }
            extensions.put(key, value);
            return this;
        }

        public CommandProblem build() {
            return new CommandProblem(title, detail, extensions);
        }
    }
}
