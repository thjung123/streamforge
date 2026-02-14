package com.streamforge.core.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public final class ScopedConfig {

  static final String COMMON_KEY = "common";

  private static final Dotenv dotenv;
  private static final Map<String, Map<String, String>> jsonConfig;
  private static volatile String activeJob;

  static {
    dotenv = Dotenv.configure().directory(".").filename(".env").ignoreIfMissing().load();
    jsonConfig = loadJsonConfig("streamforge.json");

    System.out.println("[CONFIG] dotenv loaded: " + dotenv.entries().size() + " entries");
    System.out.println("[CONFIG] json loaded: " + jsonConfig.size() + " sections");
  }

  private ScopedConfig() {}

  public static void activateJob(String jobName) {
    activeJob = jobName;
    System.out.println("[CONFIG] active job: " + jobName);
  }

  public static String activeJob() {
    return activeJob;
  }

  /** Resolution order: System property → env var → .env → JSON[activeJob] → JSON[common] */
  public static String require(String key) {
    String value = resolve(key);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required config: " + key);
    }
    return value;
  }

  public static String getOrDefault(String key, String defaultValue) {
    String value = resolve(key);
    return (value != null && !value.isBlank()) ? value : defaultValue;
  }

  public static boolean exists(String key) {
    String value = resolve(key);
    return value != null && !value.isBlank();
  }

  private static String resolve(String key) {
    // 1. System property
    String value = System.getProperty(key);
    if (value != null) return value;

    // 2. Environment variable
    value = System.getenv(key);
    if (value != null) return value;

    // 3. .env file
    value = dotenv.get(key);
    if (value != null) return value;

    // 4. JSON — active job section
    if (activeJob != null) {
      Map<String, String> jobSection = jsonConfig.get(activeJob);
      if (jobSection != null) {
        value = jobSection.get(key);
        if (value != null) return value;
      }
    }

    // 5. JSON — common section
    Map<String, String> commonSection = jsonConfig.get(COMMON_KEY);
    if (commonSection != null) {
      value = commonSection.get(key);
      if (value != null) return value;
    }

    return null;
  }

  static Map<String, Map<String, String>> loadJsonConfig(String path) {
    File file = new File(path);
    if (!file.exists()) {
      return Collections.emptyMap();
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(file, new TypeReference<>() {});
    } catch (IOException e) {
      System.err.println("[CONFIG] Failed to load " + path + ": " + e.getMessage());
      return Collections.emptyMap();
    }
  }
}
