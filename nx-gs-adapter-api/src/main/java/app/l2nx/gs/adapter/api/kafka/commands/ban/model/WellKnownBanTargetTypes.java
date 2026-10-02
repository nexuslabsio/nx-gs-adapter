package app.l2nx.gs.adapter.api.kafka.commands.ban.model;

import app.l2nx.gs.adapter.api.kafka.commands.ban.BanCommand;
import app.l2nx.gs.adapter.api.kafka.commands.ban.UnbanCommand;

/**
 * Canonical {@code targetType} values on {@link BanCommand} / {@link UnbanCommand} and
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.ban.BanDbDto}. Open string so a new host dimension is not a
 * breaking change; unknown values are stored verbatim. {@code UPPER_SNAKE_CASE}.
 *
 * <ul>
 *   <li>{@link #CHARACTER} - {@code targetValue} is the char id as a string.</li>
 *   <li>{@link #ACCOUNT} - whole login account; {@code targetValue} is the account login.</li>
 *   <li>{@link #IP} - plaintext IP.</li>
 *   <li>{@link #HWID} - HWID hash.</li>
 *   <li>{@link #HARD} - command-only fan-out marker: the host expands it into character + account + IP + HWID
 *   bans, so a persisted ban row never carries {@code HARD}.</li>
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
