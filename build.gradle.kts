plugins {
    base
    alias(libs.plugins.spotless) apply false
}

allprojects {
    group = "app.l2nx"
}

// `libs` is not visible inside `subprojects {}`, so resolve it at root scope.
val palantirVersion = libs.versions.palantir.get()

subprojects {
    apply(plugin = "com.diffplug.spotless")

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        // Gate only files changed vs origin/master.
        ratchetFrom("origin/master")
        java {
            target("src/**/*.java")
            palantirJavaFormat(palantirVersion)
        }
    }

    // Publishing builds the javadoc jar; failing it here keeps a broken doc from reaching a release tag.
    plugins.withId("java") {
        tasks.named("check") { dependsOn("javadoc") }
    }
}
