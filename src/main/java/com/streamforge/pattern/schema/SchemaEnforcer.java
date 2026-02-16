package com.streamforge.pattern.schema;

import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.core.util.JsonUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaEnforcer<T> implements PipelineBuilder.StreamPattern<T> {

  private final PayloadExtractor<T> payloadExtractor;
  private final List<SchemaVersion> versions;

  public SchemaEnforcer(PayloadExtractor<T> payloadExtractor, List<SchemaVersion> versions) {
    this.payloadExtractor = payloadExtractor;
    this.versions = new ArrayList<>(versions);
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .process(new SchemaValidationFunction<>(payloadExtractor, versions, name()))
        .name(name());
  }

  @FunctionalInterface
  public interface PayloadExtractor<T> extends Serializable {
    Map<String, Object> extract(T value);
  }

  static class SchemaValidationFunction<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(SchemaEnforcer.class);

    private final PayloadExtractor<T> payloadExtractor;
    private final List<SchemaVersion> versions;
    private final String operatorName;
    private transient Metrics metrics;

    SchemaValidationFunction(
        PayloadExtractor<T> payloadExtractor, List<SchemaVersion> versions, String operatorName) {
      this.payloadExtractor = payloadExtractor;
      this.versions = new ArrayList<>(versions);
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "schema", operatorName);
      DLQPublisher.getInstance().initMetrics(getRuntimeContext(), "schema");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      Map<String, Object> payload = payloadExtractor.extract(value);
      List<String> allViolations = new ArrayList<>();

      for (int i = versions.size() - 1; i >= 0; i--) {
        List<String> violations = versions.get(i).validate(payload);
        if (violations.isEmpty()) {
          metrics.inc(MetricKeys.SCHEMA_PASS_COUNT);
          out.collect(value);
          return;
        }
        allViolations.add("[v" + (i + 1) + "] " + String.join(", ", violations));
      }

      metrics.inc(MetricKeys.SCHEMA_VIOLATION_COUNT);
      log.warn("[SchemaEnforcer] Dropped record with violations: {}", allViolations);

      DlqEvent dlqEvent =
          DlqEvent.of(
              ErrorCodes.SCHEMA_VIOLATION,
              String.join("; ", allViolations),
              operatorName,
              JsonUtils.toJson(value),
              null);
      DLQPublisher.getInstance().publish(dlqEvent);
    }
  }
}
