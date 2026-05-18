package app.l2nx.gs.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Slf4jNxLog implements NxLog {

    private static final String PREFIX = "[L2NX] ";

    private final Logger logger;

    Slf4jNxLog(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    @Override
    public void debug(String message, Object... args) {
        logger.debug(PREFIX + message, args);
    }

    @Override
    public void info(String message, Object... args) {
        logger.info(PREFIX + message, args);
    }

    @Override
    public void warn(String message, Object... args) {
        logger.warn(PREFIX + message, args);
    }

    @Override
    public void error(String message, Object... args) {
        logger.error(PREFIX + message, args);
    }
}
