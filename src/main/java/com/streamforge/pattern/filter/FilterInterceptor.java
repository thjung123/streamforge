package com.streamforge.pattern.filter;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FilterInterceptor<T> implements PipelineBuilder.StreamPattern<T> {

  private final SerializablePredicate<T> predicate;

  public FilterInterceptor(SerializablePredicate<T> predicate) {
    this.predicate = predicate;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream.filter(new MetricFilter<>(predicate, name())).name(name());
  }

  static class MetricFilter<T> extends RichFilterFunction<T> {

    private final SerializablePredicate<T> predicate;
    private final String operatorName;
    private transient Metrics metrics;

    MetricFilter(SerializablePredicate<T> predicate, String operatorName) {
      this.predicate = predicate;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "filter", operatorName);
    }

    @Override
    public boolean filter(T value) {
      if (predicate.test(value)) {
        metrics.inc(MetricKeys.FILTER_PASS_COUNT);
        return true;
      }
      metrics.inc(MetricKeys.FILTER_DROP_COUNT);
      return false;
    }
  }
}
