plugins {
    `java-library`
    `maven-publish`
    signing
}

version = findProperty("${project.name}.version") as String? ?: "0.3.0"

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
    api(project(":nx-gs-adapter-api"))
    implementation(project(":nx-gs-kafka"))
    implementation(project(":nx-gs-commons"))
    implementation(libs.fastutil.core)
    implementation(libs.gson)
    compileOnly(libs.slf4j.api)

    // :nx-gs-log is shadow-included into the published jar — not exposed as a Maven dep.
    compileOnly(project(":nx-gs-log"))
    testImplementation(project(":nx-gs-log"))

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.mysql)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.kafka.clients)
    testRuntimeOnly(libs.mysql.connector)
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
    manifest {
        attributes("Implementation-Version" to project.version)
    }
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
            artifactId = "nx-gs-db-sync-core"

            pom {
                name.set("nx-gs-db-sync-core")
                description.set("L2NX game-server adapter — DB sync module (Tier-1 AdapterModule, CRC32 CDC engine in Phase 2)")
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
