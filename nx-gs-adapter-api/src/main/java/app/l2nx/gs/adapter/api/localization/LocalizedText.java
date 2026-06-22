package app.l2nx.gs.adapter.api.localization;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Multilingual text value — a locale-keyed map of translated strings. Mirrors the
 * platform's {@code app.l2nx.common.localization.LocalizedText} approach so the two
 * round-trip byte-for-byte across the wire.
 *
 * <p><b>Wire form is a flat object</b> keyed by locale code:
 * {@code {"en": "Great Axe", "ru": "Двуручный Топор"}} — NOT
 * {@code {"values": {...}}}. This artifact carries no JSON binder (zero runtime
 * deps), so serialization is the consumer's responsibility:</p>
 * <ul>
 *     <li>adapter side (Gson) registers a {@code TypeAdapter<LocalizedText>} that
 *     reads/writes {@link #values()} as the flat object;</li>
 *     <li>platform side (Jackson) registers a module deserializing the flat object
 *     into this type (or directly into the platform {@code LocalizedText}).</li>
 * </ul>
 *
 * <p>Locales are NOT fixed — whatever languages the host build exposes are carried.
 * The value is immutable; at least one locale must hold a non-blank string.</p>
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
     * Factory mirroring the platform type's delegating creator. Returns {@code null}
     * for a {@code null} or empty input so callers can map "no localized name" to a
     * null field without tripping the non-blank invariant.
     */
    public static @Nullable LocalizedText of(@Nullable Map<String, String> values) {
        if (values == null || !hasNonBlank(values)) {
            return null;
        }
        return new LocalizedText(values);
    }

    /**
     * The locale → string map. Immutable; serialized as the flat wire object.
     */
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
