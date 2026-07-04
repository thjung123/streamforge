package com.streamforge.core.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScopedConfig {

  static final String COMMON_KEY = "common";

  private static final Dotenv dotenv;
  private static final Map<String, Map<String, String>> jsonConfig;
  private static volatile String activeJob;

  private static final Logger log = LoggerFactory.getLogger(ScopedConfig.class);

  static {
    dotenv = Dotenv.configure().directory(".").filename(".env").ignoreIfMissing().load();
    jsonConfig = loadJsonConfig("streamforge.json");

    log.info(
        "config loaded: {} dotenv entries, {} json sections",
        dotenv.entries().size(),
        jsonConfig.size());
  }

  private ScopedConfig() {}

  public static void activateJob(String jobName) {
    activeJob = jobName;
    log.info("active job: {}", jobName);
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

  public static String getGlobalOrDefault(String key, String defaultValue) {
    String value = System.getProperty(key);
    if (value == null) {
      value = System.getenv(key);
    }
    if (value == null) {
      value = dotenv.get(key);
    }
    if (value == null) {
      Map<String, String> commonSection = jsonConfig.get(COMMON_KEY);
      if (commonSection != null) {
        value = commonSection.get(key);
      }
    }
    return (value != null && !value.isBlank()) ? value : defaultValue;
  }

  public static boolean exists(String key) {
    String value = resolve(key);
    return value != null && !value.isBlank();
  }

  private static String resolve(String key) {
    String value = System.getProperty(key);
    if (value != null) return value;

    value = System.getenv(key);
    if (value != null) return value;

    value = dotenv.get(key);
    if (value != null) return value;

    if (activeJob != null) {
      Map<String, String> jobSection = jsonConfig.get(activeJob);
      if (jobSection != null) {
        value = jobSection.get(key);
        if (value != null) return value;
      }
    }

    Map<String, String> commonSection = jsonConfig.get(COMMON_KEY);
    if (commonSection != null) {
      value = commonSection.get(key);
      return value;
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
      log.error("Failed to load {}: {}", path, e.getMessage());
      return Collections.emptyMap();
    }
  }
}
