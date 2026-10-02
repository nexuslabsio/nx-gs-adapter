package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.Nullable;

final class PrivateStoreLists {

    private PrivateStoreLists() {}

    static <T> List<T> freeze(@Nullable List<T> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(src));
    }
}
