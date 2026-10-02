package app.l2nx.gs.adapter.core.kafka.gson;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Lives in adapter-core because nx-gs-kafka's Gson factory must not depend on nx-gs-adapter-api. */
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
        // of(...) returns null for an empty map instead of tripping the non-blank invariant
        return LocalizedText.of(values);
    }
}
