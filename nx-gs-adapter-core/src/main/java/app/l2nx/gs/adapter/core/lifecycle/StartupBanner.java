package app.l2nx.gs.adapter.core.lifecycle;

import app.l2nx.gs.log.NxLog;

/** Plain text only: host log sinks vary, so no ANSI codes. */
public final class StartupBanner {

    private static final String[] WORDMARK = new String[] {
        " _     ____  _   ___  __",
        "| |   |___ \\| \\ | \\ \\/ /",
        "| |     __) |  \\| |\\  / ",
        "| |___ / __/| |\\  |/  \\ ",
        "|_____|_____|_| \\_/_/\\_\\"
    };

    private StartupBanner() {}

    public static void emit(NxLog log, String version) {
        log.info("");
        for (int i = 0; i < WORDMARK.length; i++) {
            String line = WORDMARK[i];
            if (i == WORDMARK.length / 2) {
                log.info("{}    L2NX game-server adapter — version {}", line, version);
            } else {
                log.info(line);
            }
        }
        log.info("");
    }
}
