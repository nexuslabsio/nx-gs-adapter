plugins {
    `java-library`
    `maven-publish`
    signing
}

version = findProperty("${project.name}.version") as String? ?: "0.92.0"

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile> {
    options.release.set(8)
    // Java 8 target is intentional (host JVMs span 8 to 25+); -Xlint:-options mutes its obsolescence warning.
    // -parameters lets JSON binders map constructor params without @JsonProperty.
    options.compilerArgs.addAll(listOf("-Xlint:deprecation", "-Xlint:-options", "-parameters"))
}

repositories {
    mavenCentral()
}

dependencies {
    api(libs.jspecify)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.gson)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform {
        findProperty("excludeTags")?.toString()?.let { excludeTags(it) }
    }
}

// Missing-comment doclint is off; other categories stay on.
tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:-missing", "-quiet")
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
            artifactId = "nx-gs-adapter-api"

            pom {
                name.set("nx-gs-adapter-api")
                description.set("Wire contracts (DTOs + SPI) for the L2NX game-server adapter")
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
