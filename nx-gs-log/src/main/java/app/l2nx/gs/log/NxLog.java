package app.l2nx.gs.log;

/**
 * Internal logging facade. Uses SLF4J {@code {}} placeholder syntax.
 */
public interface NxLog {

    void debug(String message, Object... args);

    void info(String message, Object... args);

    void warn(String message, Object... args);

    void error(String message, Object... args);
}
