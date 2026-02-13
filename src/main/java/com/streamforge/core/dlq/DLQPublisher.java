package com.streamforge.core.dlq;

import static com.streamforge.core.config.ScopedConfig.*;

import com.streamforge.connector.kafka.KafkaConfigKeys;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.util.JsonUtils;
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
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, json);

      producer.send(
          record,
          (metadata, exception) -> {
            if (exception != null) {
              log.error("[DLQ] Failed to publish event asynchronously: {}", event, exception);
              if (metrics != null) {
                metrics.inc(MetricKeys.DLQ_FAILED_COUNT);
              }
            } else {
              if (metrics != null) {
                metrics.inc(MetricKeys.DLQ_PUBLISHED_COUNT);
              }
              if (log.isDebugEnabled()) {
                log.debug(
                    "[DLQ] Published to {} partition={} offset={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
              }
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
    return props;
  }

  private String name() {
    return "DLQPublisher";
  }
}
