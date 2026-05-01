package app.l2nx.gs.adapter.api.domain.character;

import org.jspecify.annotations.Nullable;

/**
 * Active private-store mode of a player character.
 *
 * <p>Surfaced on the wire as the open subset of L2 store states — only
 * fully-active modes are modeled: the transient menu-open states
 * ({@code SELL_PENDING}, {@code BUY_PENDING}) and the "no store" sentinel
 * map to {@code null}.</p>
 *
 * <p>Schema providers translate the source-side numeric ID
 * (canonical L2 convention: {@code 1=SELL, 3=BUY, 5=CRAFT, 8=PACKAGE_SELL})
 * into this enum via {@link #byId(int)}. Source IDs not in this set are
 * logged as warnings and surface as {@code null} on the wire.</p>
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

    /**
     * Source-side numeric store-mode ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Resolves a numeric store-mode ID into the enum constant.
     *
     * @param id source-side numeric ID
     * @return the matching constant, or {@code null} when the ID is not
     * one of the surfaced active store modes
     */
    public static @Nullable CharacterPrivateStore byId(int id) {
        if (id < 0 || id >= BY_ID.length) return null;
        return BY_ID[id];
    }
}
