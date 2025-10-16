package com.flinkcdc.common.config;


public final class ConfigKeys {

    private ConfigKeys() {}

    public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String CDC_TOPIC = "CDC_TOPIC";

    public static final String MONGO_URI = "MONGO_URI";
    public static final String MONGO_DB = "MONGO_DB";
    public static final String MONGO_COLLECTION = "MONGO_COLLECTION";

    public static final String DLQ_TOPIC = "DLQ_TOPIC";
    public static final String APP_ENV = "APP_ENV";
}
