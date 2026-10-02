package app.l2nx.gs.adapter.api;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Wire DTOs under {@code kafka.*} and {@code rest.*} must use {@link java.time.Instant} for timestamps;
 * zone-carrying and {@code java.sql.*} types are forbidden because the platform is strictly UTC.
 */
class WireTimestampConformanceTest {

    private static final Set<Class<?>> FORBIDDEN = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            java.time.OffsetDateTime.class,
            java.time.ZonedDateTime.class,
            java.time.LocalDateTime.class,
            java.time.LocalDate.class,
            java.time.LocalTime.class,
            java.util.Date.class,
            java.util.Calendar.class,
            java.sql.Date.class,
            java.sql.Time.class,
            java.sql.Timestamp.class)));

    private static final List<String> SCANNED_PACKAGES =
            Arrays.asList("app/l2nx/gs/adapter/api/kafka", "app/l2nx/gs/adapter/api/rest");

    @Test
    void wireDtoFields_shouldUseInstantOnlyForTimestamps() throws Exception {
        Path classesRoot = locateClassesRoot();
        assertTrue(
                Files.isDirectory(classesRoot),
                "Build classes directory not found at " + classesRoot + " — run `./gradlew compileJava` first.");

        List<String> violations = new ArrayList<>();
        for (String pkg : SCANNED_PACKAGES) {
            Path pkgRoot = classesRoot.resolve(pkg);
            if (!Files.isDirectory(pkgRoot)) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(pkgRoot)) {
                walk.filter(p -> p.toString().endsWith(".class"))
                        .forEach(classFile -> checkClass(classesRoot, classFile, violations));
            }
        }

        if (!violations.isEmpty()) {
            fail("Wire DTO timestamp violations — every timestamp field MUST use java.time.Instant:\n  "
                    + String.join("\n  ", violations));
        }
    }

    private static Path locateClassesRoot() {
        Path gradle = Paths.get("build/classes/java/main");
        if (Files.isDirectory(gradle)) return gradle;
        Path maven = Paths.get("target/classes");
        if (Files.isDirectory(maven)) return maven;
        return gradle;
    }

    private static void checkClass(Path classesRoot, Path classFile, List<String> violations) {
        String relPath = classesRoot.relativize(classFile).toString().replace('\\', '/');
        // Not replace(".class", ""): it would mangle package segments like gd/classtemplate.
        String className =
                relPath.substring(0, relPath.length() - ".class".length()).replace('/', '.');
        Class<?> clazz;
        try {
            clazz = Class.forName(className, false, WireTimestampConformanceTest.class.getClassLoader());
        } catch (ClassNotFoundException | NoClassDefFoundError missing) {
            // Reported as a violation so an unloadable class cannot hide a bad timestamp field.
            violations.add(className + " : could not load for inspection ("
                    + missing.getClass().getSimpleName() + ": " + missing.getMessage() + ")");
            return;
        }
        if (clazz.isInterface() || clazz.isAnnotation()) {
            return;
        }
        for (Field f : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            if (FORBIDDEN.contains(f.getType())) {
                violations.add(className + "." + f.getName()
                        + " : " + f.getType().getSimpleName()
                        + " (forbidden — use java.time.Instant)");
            }
        }
    }
}
