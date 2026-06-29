package app.l2nx.gs.adapter.api.kafka.commands.ban;

/**
 * Canonical values for the {@code targetType} field on {@link BanCommand} /
 * {@link UnbanCommand} and on
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.ban.BanDbDto}. The
 * field is an <b>open string</b> so a host shipping a new target dimension is not
 * a breaking contract change. Mirrors the {@code WellKnown*} pattern on the
 * other DTOs; the platform stores unknown values verbatim. Values are
 * {@code UPPER_SNAKE_CASE}.
 *
 * <p>{@code targetType} names the dimension the ban is keyed on; the paired
 * {@code targetValue} carries the datum (char id, account name, IP, HWID).</p>
 *
 * <ul>
 *   <li>{@link #CHARACTER} — a single character; {@code targetValue} is the char
 *   id as a string.</li>
 *   <li>{@link #ACCOUNT} — a whole login account (all its characters);
 *   {@code targetValue} is the account login.</li>
 *   <li>{@link #IP} — an IP address; {@code targetValue} is the plaintext IP.</li>
 *   <li>{@link #HWID} — a hardware id; {@code targetValue} is the HWID hash.</li>
 *   <li>{@link #HARD} — command-only fan-out marker: the host expands one
 *   {@code HARD} {@link BanCommand} into the full set of concrete bans
 *   (character + account + IP + HWID) for the same subject. A persisted
 *   ban row therefore never carries {@code HARD} — it surfaces as the
 *   concrete dimension it was expanded into. Not valid on
 *   {@link app.l2nx.gs.adapter.api.kafka.sync.db.ban.BanDbDto}.</li>
 * </ul>
 */
public final class WellKnownBanTargetTypes {

    private WellKnownBanTargetTypes() {}

    public static final String CHARACTER = "CHARACTER";
    public static final String ACCOUNT = "ACCOUNT";
    public static final String IP = "IP";
    public static final String HWID = "HWID";
    public static final String HARD = "HARD";
}
