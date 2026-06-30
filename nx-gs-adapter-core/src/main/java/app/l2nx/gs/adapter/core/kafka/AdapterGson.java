package app.l2nx.gs.adapter.core.kafka;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import app.l2nx.gs.kafka.NxGsonAdapters;
import com.google.gson.Gson;

/**
 * Single source of truth for the Gson used on both sides of the adapter wire —
 * the outbound producer ({@link DefaultKafkaFactory}) and the inbound commands
 * consumer ({@code CommandsBootstrap}). Combines the {@link NxGsonAdapters}
 * java.time adapters with the {@link LocalizedText} flat-object adapter.
 *
 * <p>Both sides MUST share this so the wire stays symmetric: a divergence (the
 * consumer built a bare {@code new GsonBuilder()} while the producer registered
 * the {@link java.time.Instant} adapter) left {@code BanCommand.expiresAt}
 * decoding via Gson's reflective adapter, which fails on JDK 16+ with
 * {@code "Failed making field java.time.Instant#seconds accessible"}.</p>
 */
public final class AdapterGson {

    private AdapterGson() {}

    public static Gson create() {
        return NxGsonAdapters.builder()
                .registerTypeAdapter(LocalizedText.class, new LocalizedTextTypeAdapter())
                .create();
    }
}
