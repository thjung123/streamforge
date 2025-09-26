package com.flinkcdc.common.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class ScopedConfig {

    private static final Dotenv dotenv;

    static {
        String env = System.getenv("APP_ENV");
        String envFile = env != null ? ".env." + env : ".env";
        dotenv = Dotenv.configure()
                .directory("src/main/resources")
                .filename(envFile)
                .ignoreIfMissing()
                .load();
    }

    private ScopedConfig() {}

    public static String require(String key) {
        String value = dotenv.get(key, System.getenv(key));
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required config: " + key);
        }
        return value;
    }

    public static String getOrDefault(String key, String defaultValue) {
        return dotenv.get(key, System.getenv(key) != null ? System.getenv(key) : defaultValue);
    }

    public static boolean exists(String key) {
        return dotenv.get(key) != null || System.getenv(key) != null;
    }
}
