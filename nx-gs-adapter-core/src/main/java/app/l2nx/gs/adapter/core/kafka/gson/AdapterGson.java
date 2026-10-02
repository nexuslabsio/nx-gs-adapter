package app.l2nx.gs.adapter.core.kafka.gson;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import app.l2nx.gs.kafka.NxGsonAdapters;
import com.google.gson.Gson;

/** Shared by producer and commands consumer so the wire stays symmetric; a bare GsonBuilder on one side decodes Instant reflectively, which fails on JDK 16+. */
public final class AdapterGson {

    private AdapterGson() {}

    public static Gson create() {
        return NxGsonAdapters.builder()
                .registerTypeAdapter(LocalizedText.class, new LocalizedTextTypeAdapter())
                .create();
    }
}
