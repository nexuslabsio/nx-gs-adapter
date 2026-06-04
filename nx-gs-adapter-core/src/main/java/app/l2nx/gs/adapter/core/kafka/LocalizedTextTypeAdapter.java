package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Gson {@link TypeAdapter} for {@link LocalizedText}: reads / writes the flat
 * locale-keyed object form ({@code {"en": "Great Axe", "ru": "Двуручный Топор"}})
 * matching the platform's {@code LocalizedText} wire shape.
 *
 * <p>Lives in adapter-core (not {@code nx-gs-kafka}) because the Gson factory in
 * {@code nx-gs-kafka} must stay free of any {@code nx-gs-adapter-api} dependency;
 * adapter-core depends on api and registers this adapter onto the producer Gson
 * at construction time.</p>
 */
public final class LocalizedTextTypeAdapter extends TypeAdapter<LocalizedText> {

    @Override
    public void write(JsonWriter out, LocalizedText value) throws IOException {
        if (value == null) {
            out.nullValue();
            return;
        }
        out.beginObject();
        for (Map.Entry<String, String> e : value.values().entrySet()) {
            out.name(e.getKey()).value(e.getValue());
        }
        out.endObject();
    }

    @Override
    public LocalizedText read(JsonReader in) throws IOException {
        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        }
        Map<String, String> values = new LinkedHashMap<String, String>();
        in.beginObject();
        while (in.hasNext()) {
            String locale = in.nextName();
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
            } else {
                values.put(locale, in.nextString());
            }
        }
        in.endObject();
        // of(...) returns null for an all-blank / empty map rather than tripping
        // the LocalizedText non-blank invariant on an empty wire object.
        return LocalizedText.of(values);
    }
}
