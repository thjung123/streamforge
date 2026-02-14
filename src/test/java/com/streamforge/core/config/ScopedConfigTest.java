package com.streamforge.core.config;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScopedConfigTest {

  @BeforeAll
  static void setUp() {
    System.setProperty("APP_NAME", "streamforge-pipeline");
    System.setProperty("KAFKA_BROKER", "localhost:9092");
  }

  @Test
  @DisplayName("require() should return the value for an existing key")
  void testRequireReturnsValue() {
    String appName = ScopedConfig.require("APP_NAME");
    assertEquals("streamforge-pipeline", appName);
  }

  @Test
  @DisplayName("require() should throw an exception if the key does not exist")
  void testRequireThrowsIfMissing() {
    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> ScopedConfig.require("NOT_EXIST"));
    assertTrue(ex.getMessage().contains("NOT_EXIST"));
  }

  @Test
  @DisplayName(
      "getOrDefault() should return the value if the key exists, or the default value if it does not")
  void testGetOrDefault() {
    assertEquals("localhost:9092", ScopedConfig.getOrDefault("KAFKA_BROKER", "default"));
    assertEquals("fallback", ScopedConfig.getOrDefault("NOT_EXIST", "fallback"));
  }

  @Test
  @DisplayName("exists() should accurately report whether a key is present")
  void testExists() {
    assertTrue(ScopedConfig.exists("APP_NAME"));
    assertTrue(ScopedConfig.exists("KAFKA_BROKER"));
    assertFalse(ScopedConfig.exists("NOT_EXIST"));
  }

  // --- JSON resolution tests ---

  @Test
  @DisplayName("loadJsonConfig() should parse valid JSON into section map")
  void testLoadJsonConfig() throws IOException {
    File tmpFile = File.createTempFile("config-", ".json");
    tmpFile.deleteOnExit();
    Files.writeString(
        tmpFile.toPath(),
        """
        {
          "common": { "KEY_A": "common-a", "KEY_B": "common-b" },
          "TestJob": { "KEY_A": "job-a", "KEY_C": "job-c" }
        }
        """);

    Map<String, Map<String, String>> config =
        ScopedConfig.loadJsonConfig(tmpFile.getAbsolutePath());

    assertEquals(2, config.size());
    assertEquals("common-a", config.get("common").get("KEY_A"));
    assertEquals("job-a", config.get("TestJob").get("KEY_A"));
    assertEquals("job-c", config.get("TestJob").get("KEY_C"));
  }

  @Test
  @DisplayName("loadJsonConfig() should return empty map when file is missing")
  void testLoadJsonConfigMissingFile() {
    Map<String, Map<String, String>> config =
        ScopedConfig.loadJsonConfig("/nonexistent/config.json");
    assertTrue(config.isEmpty());
  }

  @Test
  @DisplayName("activateJob() should set and expose the active job name")
  void testActivateJob() {
    ScopedConfig.activateJob("MyJob");
    assertEquals("MyJob", ScopedConfig.activeJob());
  }

  @Test
  @DisplayName("System property should override JSON config value")
  void testSystemPropertyOverridesJson() {
    // DLQ_TOPIC is in streamforge.json common, but set via system property here
    System.setProperty("DLQ_TOPIC", "sys-dlq");
    try {
      assertEquals("sys-dlq", ScopedConfig.require("DLQ_TOPIC"));
    } finally {
      System.clearProperty("DLQ_TOPIC");
    }
  }

  @Test
  @DisplayName("JSON job section should override JSON common section")
  void testJsonJobOverridesCommon() {
    // STREAM_TOPIC exists in MongoToKafka and KafkaToMongo but not in common
    // KAFKA_BOOTSTRAP_SERVERS exists in common
    ScopedConfig.activateJob("MongoToKafka");
    try {
      assertEquals("mongo-events", ScopedConfig.require("STREAM_TOPIC"));
      assertEquals("source_db", ScopedConfig.require("MONGO_DB"));
    } finally {
      ScopedConfig.activateJob(null);
    }
  }

  @Test
  @DisplayName("JSON common values should be accessible when no job is active")
  void testJsonCommonFallback() {
    ScopedConfig.activateJob(null);
    // DLQ_TOPIC is only in JSON common (not in .env after simplification)
    assertEquals("stream-dlq", ScopedConfig.require("DLQ_TOPIC"));
  }

  @Test
  @DisplayName("Switching active job should change resolved values")
  void testSwitchingJobs() {
    ScopedConfig.activateJob("MongoToKafka");
    assertEquals("source_db", ScopedConfig.require("MONGO_DB"));

    ScopedConfig.activateJob("KafkaToMongo");
    assertEquals("sink_db", ScopedConfig.require("MONGO_DB"));

    ScopedConfig.activateJob(null);
  }
}
