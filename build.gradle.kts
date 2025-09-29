plugins {
    id("java")
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

dependencies {
    // Flink core
    implementation("org.apache.flink:flink-streaming-java:1.19.0")
    implementation("org.apache.flink:flink-clients:1.19.0")
    implementation("org.apache.flink:flink-connector-base:1.19.0")
    implementation("org.apache.flink:flink-connector-kafka:3.2.0-1.19")

    // Utilities
    implementation("org.jsoup:jsoup:1.17.2")
    implementation("io.github.cdimascio:dotenv-java:3.0.0")

    // External clients
    implementation("io.lettuce:lettuce-core:6.3.0.RELEASE")
    implementation("redis.clients:jedis:3.6.3")
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.1")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.17.1")

    implementation("org.apache.flink:flink-connector-mongodb:1.2.0-1.18")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.36")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok:1.18.32")
    testCompileOnly("org.projectlombok:lombok:1.18.32")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.32")

    // Unit Test
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.11.0")
    implementation("org.apache.flink:flink-connector-datagen:1.19.0")

    testImplementation("org.mockito:mockito-inline:5.2.0")
    testImplementation("org.assertj:assertj-core:3.26.0")
    // Testcontainers
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
    testImplementation("org.testcontainers:kafka:1.20.1")
    testImplementation("org.testcontainers:mongodb:1.20.1")
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
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("PASSED", "FAILED", "SKIPPED")
    }
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
    archiveBaseName.set("app-all")
    archiveVersion.set("")
    archiveClassifier.set("")

    manifest {
        attributes["Main-Class"] = "com.flinkcdc.App"
    }

    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
