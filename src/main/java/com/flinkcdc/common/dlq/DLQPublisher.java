package com.flinkcdc.common.dlq;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.utils.JsonUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class DLQPublisher {

    private static volatile DLQPublisher instance;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private static final Logger log = LoggerFactory.getLogger(DLQPublisher.class);

    private transient Metrics metrics;

    private DLQPublisher() {
        this.topic = require(DLQ_TOPIC);
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
            Future<RecordMetadata> future = producer.send(record);
            future.get();

            if (metrics != null) {
                metrics.inc(MetricKeys.DLQ_PUBLISHED_COUNT);
            }
        } catch (Exception e) {
            log.error("[DLQ] Failed to publish event: {}", event, e);
            if (metrics != null) {
                metrics.inc(MetricKeys.DLQ_FAILED_COUNT);
            }
        }
    }

    private static Properties props() {
        Properties props = new Properties();
        props.put("bootstrap.servers", require(KAFKA_BOOTSTRAP_SERVERS));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private String name() {
        return "DLQPublisher";
    }
}
