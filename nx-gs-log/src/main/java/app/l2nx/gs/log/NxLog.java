package app.l2nx.gs.log;

public interface NxLog {

    void debug(String message, Object... args);

    void info(String message, Object... args);

    void warn(String message, Object... args);

    void error(String message, Object... args);
}
