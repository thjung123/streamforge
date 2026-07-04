package com.streamforge.connector.kafka;

public final class KafkaConfigKeys {

  private KafkaConfigKeys() {}

  public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
  public static final String STREAM_TOPIC = "STREAM_TOPIC";
  public static final String DLQ_TOPIC = "DLQ_TOPIC";

  public static final String DELIVERY_MODE = "DELIVERY_MODE";

  public static final String DELIVERY_AT_LEAST_ONCE = "at_least_once";
  public static final String DELIVERY_EXACTLY_ONCE = "exactly_once";
}
