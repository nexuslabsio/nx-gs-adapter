package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Package-private collection-freezing helpers shared by the private-store
 * command DTOs. Defensive copy + unmodifiable wrap; null/empty input collapses
 * to {@link Collections#emptyList()} / {@link Collections#emptyMap()}.
 */
final class PrivateStoreLists {

    private PrivateStoreLists() {}

    static <T> List<T> freeze(@Nullable List<T> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(src));
    }

    static Map<Attribute, Integer> freezeAttributes(@Nullable Map<Attribute, Integer> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new EnumMap<Attribute, Integer>(src));
    }
}
