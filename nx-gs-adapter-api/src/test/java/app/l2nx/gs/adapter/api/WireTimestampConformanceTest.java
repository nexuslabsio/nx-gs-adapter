package app.l2nx.gs.adapter.api;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Enforces the platform-wide UTC contract: every field in any wire DTO
 * (under {@code app.l2nx.gs.adapter.api.kafka.*} and
 * {@code app.l2nx.gs.adapter.api.rest.*}) MUST use {@link java.time.Instant}
 * for timestamps. {@link java.time.OffsetDateTime},
 * {@link java.time.ZonedDateTime}, {@link java.time.LocalDateTime},
 * {@link java.time.LocalDate}, {@link java.time.LocalTime},
 * {@link java.util.Date}, {@link java.util.Calendar},
 * and {@code java.sql.*} time types are forbidden — they carry / lose
 * timezone information unpredictably, and the platform operates strictly
 * on UTC.
 *
 * <p>Client code (schema providers, tenant adapters) MUST translate
 * source-side timestamps via {@code JdbcNulls.nullableInstantFromEpochMillis}
 * or equivalent, never through {@code rs.getTimestamp().toLocalDateTime()}
 * style calls. {@link java.time.Instant} is timezone-free by construction
 * (UTC-equivalent moments) so wire serialization cannot leak host timezone.</p>
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
            java.sql.Timestamp.class
    )));

    private static final List<String> SCANNED_PACKAGES = Arrays.asList(
            "app/l2nx/gs/adapter/api/kafka",
            "app/l2nx/gs/adapter/api/rest"
    );

    @Test
    void wireDtoFields_shouldUseInstantOnlyForTimestamps() throws Exception {
        Path classesRoot = locateClassesRoot();
        assertTrue(Files.isDirectory(classesRoot),
                "Build classes directory not found at " + classesRoot
                        + " — run `./gradlew compileJava` first.");

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
        // Gradle default
        Path gradle = Paths.get("build/classes/java/main");
        if (Files.isDirectory(gradle)) return gradle;
        // Maven fallback (in case)
        Path maven = Paths.get("target/classes");
        if (Files.isDirectory(maven)) return maven;
        return gradle;
    }

    private static void checkClass(Path classesRoot, Path classFile, List<String> violations) {
        String relPath = classesRoot.relativize(classFile).toString().replace('\\', '/');
        // Strip only the trailing ".class" extension — a plain replace(".class","") would also
        // mangle package segments containing that substring (e.g. gd/classtemplate).
        String className = relPath.substring(0, relPath.length() - ".class".length()).replace('/', '.');
        Class<?> clazz;
        try {
            clazz = Class.forName(className, false, WireTimestampConformanceTest.class.getClassLoader());
        } catch (ClassNotFoundException | NoClassDefFoundError missing) {
            // Compiled class file present but its transitive dependency is not
            // on the test classpath — surface as a violation so the gap can't
            // hide a real timestamp-field issue in the unloaded class.
            violations.add(className + " : could not load for inspection (" + missing.getClass().getSimpleName()
                    + ": " + missing.getMessage() + ")");
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
