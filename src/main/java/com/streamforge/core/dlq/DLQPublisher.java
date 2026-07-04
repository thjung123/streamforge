package com.streamforge.core.dlq;

import static com.streamforge.core.config.ScopedConfig.*;

import com.streamforge.connector.kafka.KafkaConfigKeys;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.util.JsonUtils;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLQPublisher {

  private static volatile DLQPublisher instance;
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private static final Logger log = LoggerFactory.getLogger(DLQPublisher.class);

  private transient Metrics metrics;

  private DLQPublisher() {
    this.topic = require(KafkaConfigKeys.DLQ_TOPIC);
    this.producer = new KafkaProducer<>(props());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    producer.close(Duration.ofSeconds(5));
                  } catch (Exception e) {
                    log.debug("[DLQ] Producer close failed on shutdown", e);
                  }
                }));
  }

  public static DLQPublisher getInstance() {
    if (instance == null) {
      synchronized (DLQPublisher.class) {
        if (instance == null) {
          instance = new DLQPublisher();
        }
      }
    }
    return instance;
  }

  public void initMetrics(RuntimeContext ctx, String jobName) {
    this.metrics = new Metrics(ctx, jobName, name());
  }

  public void publish(DlqEvent event) {
    try {
      String json = JsonUtils.toJson(event);
      producer.send(
          new ProducerRecord<>(topic, json),
          (metadata, ex) -> {
            if (ex != null) {
              log.error("[DLQ] Failed to publish event: {}", event, ex);
              if (metrics != null) {
                metrics.inc(MetricKeys.DLQ_FAILED_COUNT);
              }
            } else if (metrics != null) {
              metrics.inc(MetricKeys.DLQ_PUBLISHED_COUNT);
            }
          });
    } catch (Exception e) {
      log.error("[DLQ] Failed to publish event: {}", event, e);
      if (metrics != null) {
        metrics.inc(MetricKeys.DLQ_FAILED_COUNT);
      }
    }
  }

  private static Properties props() {
    Properties props = new Properties();
    props.put("bootstrap.servers", require(KafkaConfigKeys.KAFKA_BOOTSTRAP_SERVERS));
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("retries", String.valueOf(Integer.MAX_VALUE));
    props.put("max.in.flight.requests.per.connection", "5");
    props.put("delivery.timeout.ms", "120000");
    props.put("max.block.ms", "2000");
    return props;
  }

  private String name() {
    return "DLQPublisher";
  }
}
