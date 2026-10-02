package app.l2nx.gs.adapter.api.domain.character;

import org.jspecify.annotations.Nullable;

/**
 * Active private-store mode. Transient menu-open states and "no store" map to {@code null}.
 *
 * <p>{@code getId()} is the canonical L2 source id ({@code 1=SELL, 3=BUY, 5=CRAFT, 8=PACKAGE_SELL}).</p>
 */
public enum CharacterPrivateStore {
    SELL(1),
    BUY(3),
    CRAFT(5),
    PACKAGE_SELL(8);

    private static final CharacterPrivateStore[] BY_ID;

    static {
        int max = 0;
        for (CharacterPrivateStore m : values()) {
            if (m.id > max) max = m.id;
        }
        CharacterPrivateStore[] table = new CharacterPrivateStore[max + 1];
        for (CharacterPrivateStore m : values()) {
            table[m.id] = m;
        }
        BY_ID = table;
    }

    private final int id;

    CharacterPrivateStore(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    /**
     * Returns {@code null} for an id that is not an active store mode.
     */
    public static @Nullable CharacterPrivateStore byId(int id) {
        if (id < 0 || id >= BY_ID.length) return null;
        return BY_ID[id];
    }
}
