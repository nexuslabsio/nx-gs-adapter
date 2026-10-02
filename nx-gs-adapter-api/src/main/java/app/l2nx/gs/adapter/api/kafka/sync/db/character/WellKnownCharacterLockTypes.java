package app.l2nx.gs.adapter.api.kafka.sync.db.character;

/**
 * Canonical, non-exhaustive values for {@link CharacterLockDbDto#getLockType()}; unknown types are stored verbatim.
 * {@link #IP}: plaintext IP; {@link #HWID}: 64-hex HWID hash; {@link #ITEM}: 64-hex HWID hash binding item-trade actions.
 */
public final class WellKnownCharacterLockTypes {

    private WellKnownCharacterLockTypes() {}

    public static final String IP = "IP";
    public static final String HWID = "HWID";
    public static final String ITEM = "ITEM";
}
