plugins {
    `java-library`
    `maven-publish`
    signing
    alias(libs.plugins.shadow)
}

version = findProperty("${project.name}.version") as String? ?: "0.20.0"

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile> {
    options.release.set(8)
    // Suppress "source/target value 8 is obsolete" — Java 8 target is intentional
    // (host JVMs span Java 8 to 25+); JDK recommends this exact flag.
    options.compilerArgs.addAll(listOf("-Xlint:deprecation", "-Xlint:-options"))
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":nx-gs-adapter-api"))
    api(project(":nx-gs-kafka"))
    api(project(":nx-gs-commons"))
    api(libs.gson)
    compileOnly(libs.slf4j.api)

    // :nx-gs-log is shadow-included into the published jar — not exposed as a Maven dep.
    compileOnly(project(":nx-gs-log"))
    testImplementation(project(":nx-gs-log"))

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.wiremock)
    testImplementation(libs.slf4j.simple)
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

// Embed :nx-gs-log compiled classes directly into the published nx-gs-adapter-core.jar
// so Maven Central consumers don't need a separate nx-gs-log dependency.
tasks.named<Jar>("jar") {
    manifest {
        attributes("Implementation-Version" to project.version)
    }
    from(project(":nx-gs-log").sourceSets["main"].output)
}

// Ship the version as a classpath resource so it survives shadow/fat-JAR repacks
// (host JVM's manifest replaces ours; Package.getImplementationVersion then returns
// null). AdapterVersion.resolve() reads this file first, manifest as fallback.
val generateVersionResource = tasks.register("generateVersionResource") {
    val outputDir = layout.buildDirectory.dir("generated/resources/version")
    outputs.dir(outputDir)
    val versionString = project.version.toString()
    inputs.property("version", versionString)
    doLast {
        val file = outputDir.get().file("META-INF/nx-gs-adapter-core.version").asFile
        file.parentFile.mkdirs()
        file.writeText(versionString)
    }
}

sourceSets["main"].resources.srcDir(generateVersionResource)

tasks.named<Jar>("sourcesJar") {
    from(project(":nx-gs-log").sourceSets["main"].allSource)
}

tasks.shadowJar {
    archiveClassifier.set("all")
    dependencies {
        exclude(dependency("org.slf4j:slf4j-api"))
    }
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
            artifactId = "nx-gs-adapter-core"

            pom {
                name.set("nx-gs-adapter-core")
                description.set("L2NX game-server adapter runtime — POST /connect, heartbeat, ServiceLoader-based module discovery")
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
