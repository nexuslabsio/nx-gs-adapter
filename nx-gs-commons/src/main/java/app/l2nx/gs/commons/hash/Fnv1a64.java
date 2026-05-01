package app.l2nx.gs.commons.hash;

/**
 * FNV-1a 64-bit non-cryptographic hash. Builder-style: {@link #start()} returns
 * the initial state; {@link #mix} chains a value into the running hash. Useful
 * for structural hashing of typed fields without intermediate string
 * serialization.
 *
 * <pre>
 * long h = Fnv1a64.start();
 * h = Fnv1a64.mix(h, dto.getId());
 * h = Fnv1a64.mix(h, dto.getCurHp());
 * </pre>
 */
public final class Fnv1a64 {

    private static final long OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long PRIME = 0x100000001b3L;

    private Fnv1a64() {
    }

    public static long start() {
        return OFFSET_BASIS;
    }

    public static long mix(long state, long value) {
        long h = state;
        for (int i = 0; i < 8; i++) {
            h ^= (value >>> (i * 8)) & 0xffL;
            h *= PRIME;
        }
        return h;
    }

    public static long mix(long state, int value) {
        return mix(state, value & 0xffffffffL);
    }

    public static long mix(long state, boolean value) {
        return mix(state, value ? 1L : 0L);
    }

    public static long mix(long state, CharSequence value) {
        long h = state;
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            h ^= c & 0xffL;
            h *= PRIME;
            h ^= (c >>> 8) & 0xffL;
            h *= PRIME;
        }
        return h;
    }
}
