package com.streamforge.pattern.observability;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

public class OnlineObserver<T> implements PipelineBuilder.StreamPattern<T> {

  private final List<QualityCheck<T>> checks;

  @SafeVarargs
  public OnlineObserver(QualityCheck<T>... checks) {
    this.checks = List.of(checks);
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream.map(new ObserverFunction<>(checks, name())).name(name());
  }

  static class ObserverFunction<T> extends RichMapFunction<T, T> {

    private final List<QualityCheck<T>> checks;
    private final String operatorName;
    private transient Metrics metrics;

    ObserverFunction(List<QualityCheck<T>> checks, String operatorName) {
      this.checks = checks;
      this.operatorName = operatorName;
    }

    ObserverFunction(List<QualityCheck<T>> checks, String operatorName, Metrics metrics) {
      this.checks = checks;
      this.operatorName = operatorName;
      this.metrics = metrics;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "observer", operatorName);
    }

    @Override
    public T map(T value) {
      metrics.inc(MetricKeys.OBSERVER_TOTAL_COUNT);

      for (QualityCheck<T> check : checks) {
        if (check.test(value)) {
          metrics.inc(check.name());
        }
      }

      return value;
    }
  }
}
