package com.flinkcdc.common.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class ScopedConfig {

    private static final Dotenv dotenv;

    static {
        dotenv = Dotenv.configure()
                .directory(".")
                .filename(".env")
                .ignoreIfMissing()
                .load();

        System.out.println("[CONFIG] dotenv loaded: " + dotenv.entries().size() + " entries");
    }

    private ScopedConfig() {}

    public static String require(String key) {
        String value = System.getProperty(key, dotenv.get(key, System.getenv(key)));
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required config: " + key);
        }
        return value;
    }

    public static String getOrDefault(String key, String defaultValue) {
        return System.getProperty(
                key,
                dotenv.get(key, System.getenv(key) != null ? System.getenv(key) : defaultValue)
        );
    }

    public static boolean exists(String key) {
        return System.getProperty(key) != null
                || dotenv.get(key) != null
                || System.getenv(key) != null;
    }
}
