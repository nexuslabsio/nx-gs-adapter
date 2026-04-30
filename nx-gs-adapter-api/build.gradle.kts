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
    // -Xlint:-options suppresses "source/target value 8 is obsolete" — Java 8 target
    // is intentional (host JVMs span Java 8 to 25+); JDK recommends this exact flag.
    // -parameters preserves constructor parameter names so JSON binders (Spring/Jackson)
    // can deserialize into the POJOs via parameter-name binding, without @JsonProperty.
    options.compilerArgs.addAll(listOf("-Xlint:deprecation", "-Xlint:-options", "-parameters"))
}

repositories {
    mavenCentral()
}

dependencies {
    api(libs.jspecify)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform {
        findProperty("excludeTags")?.toString()?.let { excludeTags(it) }
    }
}

// Silence "missing comment" javadoc warnings on getters / builder methods.
// Keeps other doclint categories active (broken @link, syntax errors, etc.).
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
