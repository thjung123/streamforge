package com.streamforge.pattern.split;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.pattern.observability.TimestampExtractor;
import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OrderedFanIn<T> {

  @FunctionalInterface
  public interface SourceTagger<T> extends Serializable {
    T tag(String sourceName, T element);
  }

  private final TimestampExtractor<T> timestampExtractor;
  private final Duration maxDrift;
  private final Map<String, DataStream<T>> sources;
  private final SourceTagger<T> sourceTagger;

  private OrderedFanIn(
      TimestampExtractor<T> timestampExtractor,
      Duration maxDrift,
      Map<String, DataStream<T>> sources,
      SourceTagger<T> sourceTagger) {
    this.timestampExtractor = timestampExtractor;
    this.maxDrift = maxDrift;
    this.sources = new LinkedHashMap<>(sources);
    this.sourceTagger = sourceTagger;
  }

  public static <T> Builder<T> builder(TimestampExtractor<T> timestampExtractor) {
    if (timestampExtractor == null) {
      throw new IllegalArgumentException("TimestampExtractor must not be null");
    }
    return new Builder<>(timestampExtractor);
  }

  public DataStream<T> merge() {
    TimestampExtractor<T> extractor = this.timestampExtractor;
    SourceTagger<T> tagger = this.sourceTagger;
    DataStream<T> first = null;
    @SuppressWarnings("unchecked")
    DataStream<T>[] rest = new DataStream[sources.size() - 1];
    int idx = 0;

    for (Map.Entry<String, DataStream<T>> entry : sources.entrySet()) {
      String name = entry.getKey();
      DataStream<T> stream = entry.getValue();

      DataStream<T> withWatermarks =
          stream.assignTimestampsAndWatermarks(
              WatermarkStrategy.<T>forBoundedOutOfOrderness(this.maxDrift)
                  .withTimestampAssigner(
                      (SerializableTimestampAssigner<T>)
                          (element, recordTimestamp) -> extractor.extract(element).toEpochMilli()));

      DataStream<T> tagged;
      if (tagger != null) {
        tagged = withWatermarks.map(element -> tagger.tag(name, element));
      } else {
        tagged = withWatermarks;
      }

      if (first == null) {
        first = tagged;
      } else {
        rest[idx++] = tagged;
      }
    }

    return first.union(rest).process(new MergeFunction<>()).name("OrderedFanIn");
  }

  public static final class Builder<T> {

    private final TimestampExtractor<T> timestampExtractor;
    private final Map<String, DataStream<T>> sources = new LinkedHashMap<>();
    private Duration maxDrift;
    private SourceTagger<T> sourceTagger;

    private Builder(TimestampExtractor<T> timestampExtractor) {
      this.timestampExtractor = timestampExtractor;
    }

    public Builder<T> source(String name, DataStream<T> stream) {
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("Source name must not be null or blank");
      }
      if (stream == null) {
        throw new IllegalArgumentException("DataStream must not be null");
      }
      if (sources.containsKey(name)) {
        throw new IllegalArgumentException("Duplicate source name: " + name);
      }
      sources.put(name, stream);
      return this;
    }

    public Builder<T> maxDrift(Duration maxDrift) {
      this.maxDrift = maxDrift;
      return this;
    }

    public Builder<T> sourceTag(SourceTagger<T> sourceTagger) {
      this.sourceTagger = sourceTagger;
      return this;
    }

    public OrderedFanIn<T> build() {
      if (maxDrift == null) {
        throw new IllegalArgumentException("maxDrift must be set");
      }
      if (sources.size() < 2) {
        throw new IllegalArgumentException("At least two sources are required");
      }
      return new OrderedFanIn<>(timestampExtractor, maxDrift, sources, sourceTagger);
    }
  }

  static class MergeFunction<T> extends ProcessFunction<T, T> {

    private transient Metrics metrics;

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "fanin", "OrderedFanIn");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      metrics.inc(MetricKeys.FANIN_MERGED_COUNT);
      out.collect(value);
    }
  }
}
