package app.l2nx.gs.commons;

import org.jspecify.annotations.Nullable;

public final class Nulls {

    private Nulls() {}

    public static @Nullable Integer zeroToNull(int raw) {
        return raw == 0 ? null : raw;
    }

    public static @Nullable Integer zeroToNull(@Nullable Integer raw) {
        return raw == null || raw == 0 ? null : raw;
    }

    public static @Nullable Long zeroToNull(long raw) {
        return raw == 0L ? null : raw;
    }

    public static @Nullable Long zeroToNull(@Nullable Long raw) {
        return raw == null || raw == 0L ? null : raw;
    }
}
