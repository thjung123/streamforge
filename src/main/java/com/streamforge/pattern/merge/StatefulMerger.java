package com.streamforge.pattern.merge;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatefulMerger<T> implements PipelineBuilder.StreamPattern<T> {

  private final KeySelector<T, String> keySelector;
  private final PayloadExtractor<T> payloadExtractor;
  private final Set<String> excludedFields;
  private final Duration ttl;

  public StatefulMerger(
      KeySelector<T, String> keySelector,
      PayloadExtractor<T> payloadExtractor,
      Set<String> excludedFields,
      Duration ttl) {
    this.keySelector = keySelector;
    this.payloadExtractor = payloadExtractor;
    this.excludedFields = excludedFields;
    this.ttl = ttl;
  }

  public StatefulMerger(
      KeySelector<T, String> keySelector,
      PayloadExtractor<T> payloadExtractor,
      Set<String> excludedFields) {
    this(keySelector, payloadExtractor, excludedFields, Duration.ofHours(24));
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .keyBy(keySelector)
        .process(new MergeFunction<>(payloadExtractor, excludedFields, ttl, name()))
        .returns(stream.getType())
        .name(name());
  }

  @FunctionalInterface
  public interface PayloadExtractor<T> extends Serializable {
    Map<String, Object> extract(T value);
  }

  static class MergeFunction<T> extends KeyedProcessFunction<String, T, T> {

    private static final Logger log = LoggerFactory.getLogger(StatefulMerger.class);

    private final PayloadExtractor<T> payloadExtractor;
    private final Set<String> excludedFields;
    private final Duration ttl;
    private final String operatorName;
    private transient ValueState<Long> hashState;
    private transient Metrics metrics;

    MergeFunction(
        PayloadExtractor<T> payloadExtractor,
        Set<String> excludedFields,
        Duration ttl,
        String operatorName) {
      this.payloadExtractor = payloadExtractor;
      this.excludedFields = excludedFields;
      this.ttl = ttl;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      var desc = new ValueStateDescriptor<>("payload-hash", Types.LONG);
      desc.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
      this.hashState = getRuntimeContext().getState(desc);
      this.metrics = new Metrics(getRuntimeContext(), "merger", operatorName);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
      long currentHash = computeHash(payloadExtractor.extract(value));
      Long previousHash = hashState.value();

      if (previousHash != null && previousHash == currentHash) {
        metrics.inc(MetricKeys.MERGER_SUPPRESSED_COUNT);
        log.debug("[StatefulMerger] Suppressed no-op update for key: {}", ctx.getCurrentKey());
        return;
      }

      hashState.update(currentHash);
      metrics.inc(MetricKeys.MERGER_PASS_COUNT);
      out.collect(value);
    }

    private long computeHash(Map<String, Object> payload) {
      if (payload == null) return 0L;
      TreeMap<String, Object> sorted = new TreeMap<>();
      for (Map.Entry<String, Object> entry : payload.entrySet()) {
        if (!excludedFields.contains(entry.getKey())) {
          sorted.put(entry.getKey(), entry.getValue());
        }
      }
      int h = Objects.hash(sorted.entrySet().toArray());
      return ((long) h << 32) | (Integer.toUnsignedLong(sorted.toString().hashCode()));
    }
  }
}
