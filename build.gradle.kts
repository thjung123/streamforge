import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("java")
    id("com.diffplug.spotless") version "7.0.2"
    id("net.ltgt.errorprone") version "4.1.0"
}

spotless {
    java {
        target("src/*/java/**/*.java")
        googleJavaFormat("1.19.2")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val flinkVersion = "1.20.0"

dependencies {
    // Flink core
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.20")
    implementation("org.apache.flink:flink-connector-mongodb:1.2.0-1.18")
    implementation("org.apache.flink:flink-connector-elasticsearch7:3.1.0-1.18")
    implementation("org.apache.flink:flink-connector-datagen:$flinkVersion")

    // Utilities
    implementation("org.jsoup:jsoup:1.18.1")
    implementation("io.github.cdimascio:dotenv-java:3.1.0")

    // External clients
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
    implementation("redis.clients:jedis:5.1.0")
    implementation("org.mongodb:mongodb-driver-sync:4.11.4")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.18.2")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.36")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")

    // ErrorProne
    errorprone("com.google.errorprone:error_prone_core:2.36.0")

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.36")
    annotationProcessor("org.projectlombok:lombok:1.18.36")
    testCompileOnly("org.projectlombok:lombok:1.18.36")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.36")

    // Unit Test
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-test-utils-junit:$flinkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.3")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.mockito:mockito-junit-jupiter:5.14.2")
    testImplementation("org.mockito:mockito-inline:5.2.0")
    testImplementation("org.assertj:assertj-core:3.27.0")
    testImplementation("org.awaitility:awaitility:4.2.2")

    // Testcontainers
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.testcontainers:kafka:1.21.4")
    testImplementation("org.testcontainers:mongodb:1.21.4")
    testImplementation("org.testcontainers:elasticsearch:1.21.4")
}

// Force transitive dependency overrides for CVEs
configurations.all {
    resolutionStrategy {
        force(
            "org.apache.commons:commons-compress:1.27.1",
            "org.apache.commons:commons-lang3:3.17.0",
            "commons-io:commons-io:2.18.0",
            "io.netty:netty-handler:4.1.115.Final",
            "io.netty:netty-common:4.1.115.Final",
            "io.netty:netty-codec:4.1.115.Final",
            "io.netty:netty-transport:4.1.115.Final",
            "io.netty:netty-buffer:4.1.115.Final",
            "io.netty:netty-resolver:4.1.115.Final"
        )
    }
}

sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")

        compileClasspath += files(
            sourceSets["main"].output,
            configurations.testRuntimeClasspath.get()
        )
        runtimeClasspath += files(
            output,
            compileClasspath
        )
    }
}

configurations {
    getByName("integrationTestImplementation").extendsFrom(configurations.testImplementation.get())
    getByName("integrationTestRuntimeOnly").extendsFrom(configurations.testRuntimeOnly.get())
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    shouldRunAfter("test")
    useJUnitPlatform()

    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.errorprone {
        disableWarningsInGeneratedCode.set(true)
        disable("MissingSummary", "InlineMeSuggester")
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED"
    )
    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
    }
}

tasks.withType<JavaExec> {
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED"
    )
}

tasks.named<Test>("integrationTest") {
    useJUnitPlatform()
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
    }
}

tasks.jar {
    archiveBaseName.set("streamforge")
    archiveVersion.set("")
    archiveClassifier.set("")

    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
