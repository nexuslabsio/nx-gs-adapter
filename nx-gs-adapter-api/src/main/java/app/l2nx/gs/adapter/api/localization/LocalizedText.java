package app.l2nx.gs.adapter.api.localization;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Locale-keyed map of translated strings, wire-compatible with the platform's
 * {@code app.l2nx.common.localization.LocalizedText}.
 *
 * <p>Wire form is a flat object ({@code {"en": "Great Axe", "ru": "..."}}), not {@code {"values": {...}}}.
 * No JSON binder is bundled; consumers register their own (Gson adapter / Jackson module).
 * Locales are open; immutable; at least one value must be non-blank.</p>
 */
public final class LocalizedText {

    private final Map<String, String> values;

    public LocalizedText(Map<String, String> values) {
        if (values == null || !hasNonBlank(values)) {
            throw new IllegalArgumentException("At least one locale must have a non-blank value");
        }
        this.values = Collections.unmodifiableMap(new LinkedHashMap<String, String>(values));
    }

    /**
     * Returns {@code null} for {@code null} or all-blank input instead of tripping the non-blank invariant.
     */
    public static @Nullable LocalizedText of(@Nullable Map<String, String> values) {
        if (values == null || !hasNonBlank(values)) {
            return null;
        }
        return new LocalizedText(values);
    }

    public Map<String, String> values() {
        return values;
    }

    public @Nullable String get(String locale) {
        return values.get(locale);
    }

    public @Nullable String getOrDefault(String locale, String fallbackLocale) {
        String value = values.get(locale);
        return value != null ? value : values.get(fallbackLocale);
    }

    private static boolean hasNonBlank(Map<String, String> values) {
        for (String v : values.values()) {
            if (v != null && !v.trim().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LocalizedText)) return false;
        LocalizedText that = (LocalizedText) o;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }

    @Override
    public String toString() {
        return "LocalizedText" + values;
    }
}
