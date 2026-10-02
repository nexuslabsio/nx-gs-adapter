package app.l2nx.gs.adapter.core.lifecycle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Resource-first: shadow / fat-JAR builds replace the manifest, so getImplementationVersion() returns null there. */
public final class AdapterVersion {

    private static final String FALLBACK = "unknown";
    private static final String RESOURCE_PATH = "/META-INF/nx-gs-adapter-core.version";

    private AdapterVersion() {}

    public static String resolve() {
        String fromResource = readResource();
        if (fromResource != null) {
            return fromResource;
        }
        String fromManifest = AdapterVersion.class.getPackage().getImplementationVersion();
        return fromManifest != null ? fromManifest : FALLBACK;
    }

    private static String readResource() {
        try (InputStream is = AdapterVersion.class.getResourceAsStream(RESOURCE_PATH)) {
            if (is == null) {
                return null;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }
                String trimmed = line.trim();
                return trimmed.isEmpty() ? null : trimmed;
            }
        } catch (IOException e) {
            return null;
        }
    }
}
