package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Package-private list-freezing helper shared by the mail-command DTOs.
 * Defensive copy + unmodifiable wrap; null/empty input collapses to
 * {@link Collections#emptyList()}.
 */
final class MailLists {

    private MailLists() {
    }

    static <T> List<T> freeze(@Nullable List<T> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<T>(src));
    }
}
