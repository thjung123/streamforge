package com.streamforge.pattern.quality;

import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.core.util.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintEnforcer<T> implements PipelineBuilder.StreamPattern<T> {

  private final List<ConstraintRule<T>> rules;

  @SafeVarargs
  public ConstraintEnforcer(ConstraintRule<T>... rules) {
    this.rules = List.of(rules);
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream.process(new ValidationFunction<>(rules, name())).name(name());
  }

  static class ValidationFunction<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(ConstraintEnforcer.class);

    private final List<ConstraintRule<T>> rules;
    private final String operatorName;
    private transient Metrics metrics;

    ValidationFunction(List<ConstraintRule<T>> rules, String operatorName) {
      this.rules = rules;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "constraint", operatorName);
      DLQPublisher.getInstance().initMetrics(getRuntimeContext(), "constraint");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      List<String> violations = new ArrayList<>();
      for (ConstraintRule<T> rule : rules) {
        String violation = rule.validate(value);
        if (violation != null) {
          violations.add(violation);
        }
      }

      if (violations.isEmpty()) {
        metrics.inc(MetricKeys.CONSTRAINT_PASS_COUNT);
        out.collect(value);
      } else {
        metrics.inc(MetricKeys.CONSTRAINT_VIOLATION_COUNT);
        log.warn("[ConstraintEnforcer] Dropped record with violations: {}", violations);

        DlqEvent dlqEvent =
            DlqEvent.of(
                ErrorCodes.CONSTRAINT_VIOLATION,
                String.join("; ", violations),
                operatorName,
                JsonUtils.toJson(value),
                null);
        DLQPublisher.getInstance().publish(dlqEvent);
      }
    }
  }
}
