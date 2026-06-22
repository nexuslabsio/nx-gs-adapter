package app.l2nx.gs.commons;

import org.jspecify.annotations.Nullable;

/**
 * Sentinel-to-null conversions. Pure transformations, no I/O. Useful when the
 * source representation uses {@code 0} as "no relation / no value" sentinel
 * (common L2J convention) and the consumer wants {@code null} instead.
 */
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
