package com.streamforge.pattern.session;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionAnalyzer<T, R> {

  @FunctionalInterface
  public interface KeyExtractor<T> extends Serializable {
    String extract(T value);
  }

  @FunctionalInterface
  public interface TimestampExtractor<T> extends Serializable {
    long extractMillis(T value);
  }

  @FunctionalInterface
  public interface Aggregator<T, R> extends Serializable {
    R aggregate(List<T> events);
  }

  private final KeyExtractor<T> keyExtractor;
  private final TimestampExtractor<T> timestampExtractor;
  private final Aggregator<T, R> aggregator;
  private final Duration gap;
  private final Duration allowedLateness;
  private final Duration outOfOrderness;

  private SessionAnalyzer(
      KeyExtractor<T> keyExtractor,
      TimestampExtractor<T> timestampExtractor,
      Aggregator<T, R> aggregator,
      Duration gap,
      Duration allowedLateness,
      Duration outOfOrderness) {
    this.keyExtractor = keyExtractor;
    this.timestampExtractor = timestampExtractor;
    this.aggregator = aggregator;
    this.gap = gap;
    this.allowedLateness = allowedLateness;
    this.outOfOrderness = outOfOrderness;
  }

  public static <T, R> Builder<T, R> builder() {
    return new Builder<>();
  }

  public DataStream<SessionResult<R>> apply(DataStream<T> stream) {
    TimestampExtractor<T> tsExtractor = this.timestampExtractor;

    var withWatermarks =
        stream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<T>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<T>)
                        (element, recordTimestamp) -> tsExtractor.extractMillis(element)));

    WindowedStream<T, String, TimeWindow> windowed =
        withWatermarks.keyBy(keyExtractor::extract).window(EventTimeSessionWindows.withGap(gap));

    if (allowedLateness != null) {
      windowed = windowed.allowedLateness(allowedLateness);
    }

    return windowed.process(new SessionProcessFunction<>(aggregator)).name("SessionAnalyzer");
  }

  public static final class Builder<T, R> {

    private KeyExtractor<T> keyExtractor;
    private TimestampExtractor<T> timestampExtractor;
    private Aggregator<T, R> aggregator;
    private Duration gap;
    private Duration allowedLateness;
    private Duration outOfOrderness = Duration.ZERO;

    private Builder() {}

    public Builder<T, R> keyExtractor(KeyExtractor<T> keyExtractor) {
      this.keyExtractor = keyExtractor;
      return this;
    }

    public Builder<T, R> timestampExtractor(TimestampExtractor<T> timestampExtractor) {
      this.timestampExtractor = timestampExtractor;
      return this;
    }

    public Builder<T, R> aggregator(Aggregator<T, R> aggregator) {
      this.aggregator = aggregator;
      return this;
    }

    public Builder<T, R> gap(Duration gap) {
      this.gap = gap;
      return this;
    }

    public Builder<T, R> allowedLateness(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
      return this;
    }

    public Builder<T, R> outOfOrderness(Duration outOfOrderness) {
      this.outOfOrderness = outOfOrderness;
      return this;
    }

    public SessionAnalyzer<T, R> build() {
      if (keyExtractor == null) {
        throw new IllegalArgumentException("keyExtractor must not be null");
      }
      if (timestampExtractor == null) {
        throw new IllegalArgumentException("timestampExtractor must not be null");
      }
      if (aggregator == null) {
        throw new IllegalArgumentException("aggregator must not be null");
      }
      if (gap == null) {
        throw new IllegalArgumentException("gap must not be null");
      }
      if (outOfOrderness == null) {
        throw new IllegalArgumentException("outOfOrderness must not be null");
      }
      return new SessionAnalyzer<>(
          keyExtractor, timestampExtractor, aggregator, gap, allowedLateness, outOfOrderness);
    }
  }

  static class SessionProcessFunction<T, R>
      extends ProcessWindowFunction<T, SessionResult<R>, String, TimeWindow> {

    private final Aggregator<T, R> aggregator;
    private transient Metrics metrics;

    SessionProcessFunction(Aggregator<T, R> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "session_analyzer", "SessionAnalyzer");
    }

    @Override
    public void process(
        String key,
        ProcessWindowFunction<T, SessionResult<R>, String, TimeWindow>.Context context,
        Iterable<T> elements,
        Collector<SessionResult<R>> out) {

      List<T> events = new ArrayList<>();
      for (T event : elements) {
        events.add(event);
      }

      TimeWindow window = context.window();
      R result = aggregator.aggregate(events);

      metrics.inc(MetricKeys.SESSION_CLOSED_COUNT);

      out.collect(
          new SessionResult<>(
              key,
              Instant.ofEpochMilli(window.getStart()),
              Instant.ofEpochMilli(window.getEnd()),
              events.size(),
              Duration.ofMillis(window.getEnd() - window.getStart()),
              result));
    }
  }
}
