package com.streamforge.connector.kafka;

import static com.streamforge.connector.kafka.KafkaConfigKeys.*;
import static com.streamforge.core.config.ErrorCodes.SINK_ERROR;
import static com.streamforge.core.config.MetricKeys.*;
import static com.streamforge.core.config.ScopedConfig.*;

import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.core.util.JsonUtils;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSinkBuilder implements PipelineBuilder.SinkBuilder<StreamEnvelop> {

  private static final Logger log = LoggerFactory.getLogger(KafkaSinkBuilder.class);
  public static final String OPERATOR_NAME = "KafkaSink";

  @Override
  public DataStreamSink<StreamEnvelop> write(DataStream<StreamEnvelop> stream, String jobName) {
    return stream.map(new MetricsMapFunction(jobName)).sinkTo(buildKafkaSink()).name(OPERATOR_NAME);
  }

  private static KafkaSink<StreamEnvelop> buildKafkaSink() {
    return KafkaSink.<StreamEnvelop>builder()
        .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<StreamEnvelop>builder()
                .setTopic(require(STREAM_TOPIC))
                .setValueSerializationSchema(envelop -> JsonUtils.toJson(envelop).getBytes())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setKafkaProducerConfig(defaultProducerConfig())
        .build();
  }

  private static Properties defaultProducerConfig() {
    Properties props = new Properties();
    props.setProperty("acks", "all");
    props.setProperty("retries", "10");
    props.setProperty("compression.type", "snappy");
    props.setProperty("batch.size", "32768");
    props.setProperty("linger.ms", "50");
    props.setProperty("max.in.flight.requests.per.connection", "1");
    props.setProperty("max.block.ms", "120000");
    props.setProperty("delivery.timeout.ms", "180000");
    props.setProperty("request.timeout.ms", "120000");
    return props;
  }

  static class MetricsMapFunction extends RichMapFunction<StreamEnvelop, StreamEnvelop> {

    private transient Metrics metrics;
    private final String jobName;

    public MetricsMapFunction(String jobName) {
      this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), jobName, KAFKA);
      DLQPublisher.getInstance().initMetrics(getRuntimeContext(), jobName);
    }

    @Override
    public StreamEnvelop map(StreamEnvelop envelop) {
      try {
        metrics.inc(SINK_SUCCESS_COUNT);
        return envelop;
      } catch (Exception e) {
        log.error("Kafka sink failed for envelop: {}", envelop, e);
        metrics.inc(SINK_ERROR_COUNT);
        DlqEvent dlqEvent =
            DlqEvent.of(
                SINK_ERROR,
                e.getMessage(),
                OPERATOR_NAME,
                envelop != null ? envelop.toJson() : null,
                e);
        DLQPublisher.getInstance().publish(dlqEvent);
        throw e;
      }
    }
  }
}
