package app.l2nx.gs.log;

public final class NxLogFactory {

    private static final boolean SLF4J_AVAILABLE;

    static {
        boolean available = false;
        try {
            // SLF4J 1.7.x: StaticLoggerBinder exists when a binding (logback, log4j-slf4j, etc.) is present
            Class.forName("org.slf4j.impl.StaticLoggerBinder");
            available = true;
        } catch (ClassNotFoundException e) {
            // SLF4J 2.x: uses ServiceLoader instead of StaticLoggerBinder
            // Check if LoggerFactory resolves to a real provider (not NOPLoggerFactory)
            try {
                Class<?> factoryClass = Class.forName("org.slf4j.LoggerFactory");
                Object loggerFactory =
                        factoryClass.getMethod("getILoggerFactory").invoke(null);
                available = !loggerFactory.getClass().getName().equals("org.slf4j.helpers.NOPLoggerFactory");
            } catch (Exception ignored) {
                // SLF4J not on classpath at all, or no provider — use console fallback
            }
        }
        SLF4J_AVAILABLE = available;
    }

    private NxLogFactory() {}

    public static NxLog getLogger(Class<?> clazz) {
        if (SLF4J_AVAILABLE) {
            try {
                return new Slf4jNxLog(clazz);
            } catch (NoClassDefFoundError e) {
                return new ConsoleNxLog(clazz);
            }
        }
        return new ConsoleNxLog(clazz);
    }

    // Visible for testing
    static boolean isSlf4jAvailable() {
        return SLF4J_AVAILABLE;
    }
}
