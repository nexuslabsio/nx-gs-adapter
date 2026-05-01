// :nx-gs-log — internal logging facade. NOT published to Maven Central; classes are
// shadow-included into :nx-gs-kafka and :nx-gs-adapter-core jars.

plugins {
    `java-library`
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
    compileOnly(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.slf4j.api)
    testImplementation(libs.slf4j.simple)
}

tasks.test {
    useJUnitPlatform {
        findProperty("excludeTags")?.toString()?.let { excludeTags(it) }
    }
}
