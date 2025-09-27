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
    implementation("org.apache.flink:flink-streaming-java:1.19.0")
    implementation("org.apache.flink:flink-clients:1.19.0")

    implementation("org.apache.flink:flink-connector-base:1.19.0")
    implementation("org.apache.flink:flink-connector-kafka:3.2.0-1.19")

    implementation("org.jsoup:jsoup:1.17.2")
    implementation("io.github.cdimascio:dotenv-java:3.0.0")

    implementation("io.lettuce:lettuce-core:6.3.0.RELEASE")
    implementation("redis.clients:jedis:3.6.3")
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.1")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.17.1")

    implementation("org.slf4j:slf4j-api:1.7.36")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")

    compileOnly("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok:1.18.32")
    testCompileOnly("org.projectlombok:lombok:1.18.32")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.32")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
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
