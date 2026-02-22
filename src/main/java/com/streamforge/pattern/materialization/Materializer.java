package com.streamforge.pattern.materialization;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Materializer<T> {

  @FunctionalInterface
  public interface KeyExtractor<T> extends Serializable {
    String extract(T value);
  }

  @FunctionalInterface
  public interface MergeFunction<T> extends Serializable {
    T merge(T existing, T incoming);
  }

  public interface DeletePredicate<T> extends Predicate<T>, Serializable {}

  private final KeyExtractor<T> keyExtractor;
  private final Duration ttl;
  private final MergeFunction<T> mergeFunction;
  private final DeletePredicate<T> deletePredicate;

  private Materializer(
      KeyExtractor<T> keyExtractor,
      Duration ttl,
      MergeFunction<T> mergeFunction,
      DeletePredicate<T> deletePredicate) {
    this.keyExtractor = keyExtractor;
    this.ttl = ttl;
    this.mergeFunction = mergeFunction;
    this.deletePredicate = deletePredicate;
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public DataStream<ChangelogEvent<T>> apply(DataStream<T> stream) {
    return stream
        .keyBy(keyExtractor::extract)
        .process(new MaterializeFunction<>(ttl, mergeFunction, deletePredicate))
        .name("Materializer");
  }

  public static final class Builder<T> {

    private KeyExtractor<T> keyExtractor;
    private Duration ttl;
    private MergeFunction<T> mergeFunction;
    private DeletePredicate<T> deletePredicate;

    private Builder() {}

    public Builder<T> keyExtractor(KeyExtractor<T> keyExtractor) {
      this.keyExtractor = keyExtractor;
      return this;
    }

    public Builder<T> ttl(Duration ttl) {
      this.ttl = ttl;
      return this;
    }

    public Builder<T> mergeFunction(MergeFunction<T> mergeFunction) {
      this.mergeFunction = mergeFunction;
      return this;
    }

    public Builder<T> deletePredicate(DeletePredicate<T> deletePredicate) {
      this.deletePredicate = deletePredicate;
      return this;
    }

    public Materializer<T> build() {
      if (keyExtractor == null) {
        throw new IllegalArgumentException("keyExtractor must not be null");
      }
      if (ttl == null) {
        throw new IllegalArgumentException("ttl must not be null");
      }
      if (mergeFunction == null) {
        mergeFunction = (existing, incoming) -> incoming;
      }
      return new Materializer<>(keyExtractor, ttl, mergeFunction, deletePredicate);
    }
  }

  static class MaterializeFunction<T> extends KeyedProcessFunction<String, T, ChangelogEvent<T>> {

    private static final Logger log = LoggerFactory.getLogger(Materializer.class);

    private final Duration ttl;
    private final MergeFunction<T> mergeFunction;
    private final DeletePredicate<T> deletePredicate;
    private transient ValueState<T> state;
    private transient Metrics metrics;

    MaterializeFunction(
        Duration ttl, MergeFunction<T> mergeFunction, DeletePredicate<T> deletePredicate) {
      this.ttl = ttl;
      this.mergeFunction = mergeFunction;
      this.deletePredicate = deletePredicate;
    }

    @Override
    public void open(Configuration parameters) {
      StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl).build();
      @SuppressWarnings({"unchecked", "rawtypes"})
      ValueStateDescriptor<T> desc =
          (ValueStateDescriptor<T>) new ValueStateDescriptor("materialized", Object.class);
      desc.enableTimeToLive(ttlConfig);
      this.state = getRuntimeContext().getState(desc);
      this.metrics = new Metrics(getRuntimeContext(), "materializer", "Materializer");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<ChangelogEvent<T>> out)
        throws Exception {
      String key = ctx.getCurrentKey();
      Instant timestamp = Instant.ofEpochMilli(ctx.timestamp() != null ? ctx.timestamp() : 0L);

      if (deletePredicate != null && deletePredicate.test(value)) {
        T before = state.value();
        state.clear();
        metrics.inc(MetricKeys.MATERIALIZER_DELETE_COUNT);
        log.debug("[Materializer] DELETE for key: {}", key);
        out.collect(
            new ChangelogEvent<>(ChangelogEvent.ChangeType.DELETE, key, before, null, timestamp));
        return;
      }

      T existing = state.value();

      if (existing == null) {
        state.update(value);
        metrics.inc(MetricKeys.MATERIALIZER_INSERT_COUNT);
        log.debug("[Materializer] INSERT for key: {}", key);
        out.collect(
            new ChangelogEvent<>(ChangelogEvent.ChangeType.INSERT, key, null, value, timestamp));
      } else {
        T merged = mergeFunction.merge(existing, value);
        state.update(merged);
        metrics.inc(MetricKeys.MATERIALIZER_UPDATE_COUNT);
        log.debug("[Materializer] UPDATE for key: {}", key);
        out.collect(
            new ChangelogEvent<>(
                ChangelogEvent.ChangeType.UPDATE, key, existing, merged, timestamp));
      }
    }
  }
}
