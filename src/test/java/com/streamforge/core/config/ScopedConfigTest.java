package com.streamforge.core.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScopedConfigTest {

  @BeforeAll
  static void setUp() {
    System.setProperty("APP_NAME", "flink-cdc-pipeline");
    System.setProperty("KAFKA_BROKER", "localhost:9092");
  }

  @Test
  @DisplayName("require() should return the value for an existing key")
  void testRequireReturnsValue() {
    String appName = ScopedConfig.require("APP_NAME");
    assertEquals("flink-cdc-pipeline", appName);
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
}
