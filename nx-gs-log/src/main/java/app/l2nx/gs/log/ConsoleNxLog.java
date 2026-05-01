package app.l2nx.gs.log;

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

final class ConsoleNxLog implements NxLog {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String PREFIX = "[Nx]";

    private final String name;

    ConsoleNxLog(Class<?> clazz) {
        this.name = clazz.getSimpleName();
    }

    @Override
    public void debug(String message, Object... args) {
        // Console fallback does not print debug messages.
        // Use SLF4J with a binding for debug-level logging.
    }

    @Override
    public void info(String message, Object... args) {
        print(System.out, "INFO", message, args);
    }

    @Override
    public void warn(String message, Object... args) {
        print(System.err, "WARN", message, args);
    }

    @Override
    public void error(String message, Object... args) {
        print(System.err, "ERROR", message, args);
    }

    private void print(PrintStream stream, String level, String message, Object[] args) {
        String timestamp = LocalDateTime.now().format(FORMATTER);
        String formatted = format(message, args);
        stream.println(PREFIX + " [" + level + "] " + timestamp + " " + name + " - " + formatted);

        // If the last arg is a Throwable and was not consumed by a placeholder, print its stack trace
        if (args != null && args.length > 0 && args[args.length - 1] instanceof Throwable) {
            int placeholders = countPlaceholders(message);
            if (placeholders < args.length) {
                ((Throwable) args[args.length - 1]).printStackTrace(stream);
            }
        }
    }

    private static String format(String message, Object[] args) {
        if (args == null || args.length == 0) {
            return message;
        }
        StringBuilder sb = new StringBuilder(message.length() + 32);
        int argIndex = 0;
        int i = 0;
        while (i < message.length()) {
            if (i < message.length() - 1
                    && message.charAt(i) == '{'
                    && message.charAt(i + 1) == '}'
                    && argIndex < args.length) {
                sb.append(args[argIndex++]);
                i += 2;
            } else {
                sb.append(message.charAt(i));
                i++;
            }
        }
        return sb.toString();
    }

    private static int countPlaceholders(String message) {
        int count = 0;
        for (int i = 0; i < message.length() - 1; i++) {
            if (message.charAt(i) == '{' && message.charAt(i + 1) == '}') {
                count++;
                i++;
            }
        }
        return count;
    }
}
