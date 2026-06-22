package app.l2nx.gs.adapter.api.kafka.sync.db.character;

/**
 * Canonical values for {@link CharacterLockDbDto#getLockType()}. The field is an
 * <b>open string</b> so a host shipping a new lock kind is not a breaking contract
 * change. Mirrors the {@code WellKnown*} pattern on the other DTOs; the set is
 * non-exhaustive and the platform stores unknown lock types verbatim. Adding a
 * constant is a non-breaking minor-version change. Values are
 * {@code UPPER_SNAKE_CASE}.
 *
 * <ul>
 *   <li>{@link #IP} — IP lock; source char-var {@code lockIp} holds the plaintext
 *   IP the character is bound to (e.g. {@code "127.0.0.1"}). Active iff the value
 *   is present, non-blank, and not the {@code "0"} sentinel.</li>
 *   <li>{@link #HWID} — HWID lock; source char-var {@code lockHwid} holds the
 *   64-hex HWID hash the character is bound to. Active iff present, non-blank,
 *   and not {@code "0"}.</li>
 *   <li>{@link #ITEM} — item-trade lock; source char-var {@code lockItem} holds a
 *   64-hex HWID hash binding item-trade actions. Active iff present, non-blank,
 *   and not {@code "0"}.</li>
 * </ul>
 */
public final class WellKnownCharacterLockTypes {

    private WellKnownCharacterLockTypes() {}

    public static final String IP = "IP";
    public static final String HWID = "HWID";
    public static final String ITEM = "ITEM";
}
