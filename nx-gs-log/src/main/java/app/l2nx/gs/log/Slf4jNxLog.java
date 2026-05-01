package app.l2nx.gs.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Slf4jNxLog implements NxLog {

    private final Logger logger;

    Slf4jNxLog(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    @Override
    public void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    @Override
    public void info(String message, Object... args) {
        logger.info(message, args);
    }

    @Override
    public void warn(String message, Object... args) {
        logger.warn(message, args);
    }

    @Override
    public void error(String message, Object... args) {
        logger.error(message, args);
    }
}
