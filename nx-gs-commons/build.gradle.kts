plugins {
    `java-library`
    `maven-publish`
    signing
}

version = findProperty("${project.name}.version") as String? ?: "0.7.0"

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile> {
    options.release.set(8)
    options.compilerArgs.addAll(listOf("-Xlint:deprecation", "-Xlint:-options"))
}

repositories {
    mavenCentral()
}

dependencies {
    api(libs.jspecify)
    api(project(":nx-gs-adapter-api"))

    // :nx-gs-log is shadow-included into the published jar — not exposed as a Maven dep.
    compileOnly(project(":nx-gs-log"))
    testImplementation(project(":nx-gs-log"))

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.mockito.core)
}

tasks.test {
    useJUnitPlatform {
        findProperty("excludeTags")?.toString()?.let { excludeTags(it) }
    }
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:-missing", "-quiet")
}

tasks.named<Jar>("jar") {
    from(project(":nx-gs-log").sourceSets["main"].output)
}

tasks.named<Jar>("sourcesJar") {
    from(project(":nx-gs-log").sourceSets["main"].allSource)
}

publishing {
    repositories {
        maven {
            name = "staging"
            url = uri(layout.buildDirectory.dir("staging-deploy"))
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "nx-gs-commons"

            pom {
                name.set("nx-gs-commons")
                description.set("Shared utilities for the L2NX game-server adapter — concurrent helpers, hash, null handling")
                url.set("https://github.com/nexuslabsio/nx-gs-adapter")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("n1rmata")
                        name.set("Kiryl Valiushka")
                        email.set("kiryl.valiushka@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/nexuslabsio/nx-gs-adapter.git")
                    developerConnection.set("scm:git:ssh://github.com:nexuslabsio/nx-gs-adapter.git")
                    url.set("https://github.com/nexuslabsio/nx-gs-adapter")
                }
            }
        }
    }
}

signing {
    val signingKey = findProperty("signingKey") as String?
    val signingPassword = findProperty("signingPassword") as String?
    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
    isRequired = signingKey != null
    sign(publishing.publications["maven"])
}
