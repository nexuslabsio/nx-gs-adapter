plugins {
    base
    alias(libs.plugins.spotless) apply false
}

allprojects {
    group = "app.l2nx"
}

// Resolved here (root-script scope) where the `libs` version-catalog accessor exists; the
// `subprojects {}` receiver is a plain Project and cannot see the generated accessor.
val palantirVersion = libs.versions.palantir.get()

subprojects {
    apply(plugin = "com.diffplug.spotless")

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        // Gate new work only (changed vs origin/master); the repo was brought to the palantir
        // baseline in one sweep when the plugin landed. Version synced with the global format-java hook.
        ratchetFrom("origin/master")
        java {
            target("src/**/*.java")
            palantirJavaFormat(palantirVersion)
        }
    }
}
