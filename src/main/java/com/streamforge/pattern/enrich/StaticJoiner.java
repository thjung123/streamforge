package com.streamforge.pattern.enrich;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.io.Serializable;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticJoiner<T, R> implements PipelineBuilder.JoinPattern<T, R> {

  @FunctionalInterface
  public interface KeyExtractor<V> extends Serializable {
    String extract(V value);
  }

  @FunctionalInterface
  public interface JoinFunction<T, R> extends Serializable {
    T enrich(T event, R refData);
  }

  private final KeyExtractor<T> mainKeyExtractor;
  private final KeyExtractor<R> refKeyExtractor;
  private final JoinFunction<T, R> joinFunction;
  private final MapStateDescriptor<String, R> descriptor;

  private StaticJoiner(
      KeyExtractor<T> mainKeyExtractor,
      KeyExtractor<R> refKeyExtractor,
      JoinFunction<T, R> joinFunction,
      MapStateDescriptor<String, R> descriptor) {
    this.mainKeyExtractor = mainKeyExtractor;
    this.refKeyExtractor = refKeyExtractor;
    this.joinFunction = joinFunction;
    this.descriptor = descriptor;
  }

  public static <T, R> Builder<T, R> builder() {
    return new Builder<>();
  }

  @Override
  public DataStream<T> join(DataStream<T> mainStream, DataStream<R> referenceStream) {
    BroadcastStream<R> broadcastStream = referenceStream.broadcast(descriptor);
    return mainStream
        .connect(broadcastStream)
        .process(
            new JoinBroadcastFunction<>(
                mainKeyExtractor, refKeyExtractor, joinFunction, descriptor))
        .name("StaticJoiner");
  }

  public static final class Builder<T, R> {

    private KeyExtractor<T> mainKeyExtractor;
    private KeyExtractor<R> refKeyExtractor;
    private JoinFunction<T, R> joinFunction;
    private MapStateDescriptor<String, R> descriptor;

    private Builder() {}

    public Builder<T, R> mainKeyExtractor(KeyExtractor<T> mainKeyExtractor) {
      this.mainKeyExtractor = mainKeyExtractor;
      return this;
    }

    public Builder<T, R> refKeyExtractor(KeyExtractor<R> refKeyExtractor) {
      this.refKeyExtractor = refKeyExtractor;
      return this;
    }

    public Builder<T, R> joinFunction(JoinFunction<T, R> joinFunction) {
      this.joinFunction = joinFunction;
      return this;
    }

    public Builder<T, R> descriptor(MapStateDescriptor<String, R> descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    public StaticJoiner<T, R> build() {
      if (mainKeyExtractor == null) {
        throw new IllegalArgumentException("mainKeyExtractor must not be null");
      }
      if (refKeyExtractor == null) {
        throw new IllegalArgumentException("refKeyExtractor must not be null");
      }
      if (joinFunction == null) {
        throw new IllegalArgumentException("joinFunction must not be null");
      }
      if (descriptor == null) {
        throw new IllegalArgumentException("descriptor must not be null");
      }
      return new StaticJoiner<>(mainKeyExtractor, refKeyExtractor, joinFunction, descriptor);
    }
  }

  static class JoinBroadcastFunction<T, R> extends BroadcastProcessFunction<T, R, T> {

    private static final Logger log = LoggerFactory.getLogger(StaticJoiner.class);

    private final KeyExtractor<T> mainKeyExtractor;
    private final KeyExtractor<R> refKeyExtractor;
    private final JoinFunction<T, R> joinFunction;
    private final MapStateDescriptor<String, R> descriptor;
    private transient Metrics metrics;

    JoinBroadcastFunction(
        KeyExtractor<T> mainKeyExtractor,
        KeyExtractor<R> refKeyExtractor,
        JoinFunction<T, R> joinFunction,
        MapStateDescriptor<String, R> descriptor) {
      this.mainKeyExtractor = mainKeyExtractor;
      this.refKeyExtractor = refKeyExtractor;
      this.joinFunction = joinFunction;
      this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "joiner", "StaticJoiner");
    }

    @Override
    public void processElement(T value, ReadOnlyContext ctx, Collector<T> out) throws Exception {
      String key = mainKeyExtractor.extract(value);
      if (key == null) {
        log.warn("[StaticJoiner] Null key extracted from event, passing through");
        out.collect(value);
        return;
      }

      ReadOnlyBroadcastState<String, R> state = ctx.getBroadcastState(descriptor);
      R refData = state.get(key);

      if (refData != null) {
        metrics.inc(MetricKeys.JOINER_MATCH_COUNT);
        out.collect(joinFunction.enrich(value, refData));
      } else {
        metrics.inc(MetricKeys.JOINER_MISS_COUNT);
        out.collect(value);
      }
    }

    @Override
    public void processBroadcastElement(R value, Context ctx, Collector<T> out) throws Exception {
      if (value == null) {
        log.warn("[StaticJoiner] Null reference data received, skipping");
        return;
      }

      String key = refKeyExtractor.extract(value);
      if (key == null) {
        log.warn("[StaticJoiner] Null key extracted from reference data, skipping");
        return;
      }

      BroadcastState<String, R> state = ctx.getBroadcastState(descriptor);
      state.put(key, value);
      metrics.inc(MetricKeys.JOINER_REF_UPDATE_COUNT);
    }
  }
}
