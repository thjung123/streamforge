package com.streamforge.pattern.dedup;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deduplicator<T> implements PipelineBuilder.StreamPattern<T> {

  private final KeySelector<T, String> keySelector;
  private final Duration ttl;

  public Deduplicator(KeySelector<T, String> keySelector, Duration ttl) {
    this.keySelector = keySelector;
    this.ttl = ttl;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .keyBy(keySelector)
        .process(new DeduplicationFunction<>(ttl, name()))
        .returns(stream.getType())
        .name(name());
  }

  static class DeduplicationFunction<T> extends KeyedProcessFunction<String, T, T> {

    private static final Logger log = LoggerFactory.getLogger(Deduplicator.class);

    private final Duration ttl;
    private final String operatorName;
    private transient ValueState<Boolean> seen;
    private transient Metrics metrics;

    DeduplicationFunction(Duration ttl, String operatorName) {
      this.ttl = ttl;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl).build();
      ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class);
      desc.enableTimeToLive(ttlConfig);
      this.seen = getRuntimeContext().getState(desc);
      this.metrics = new Metrics(getRuntimeContext(), "dedup", operatorName);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
      if (seen.value() == null) {
        seen.update(true);
        metrics.inc(MetricKeys.DEDUP_PASS_COUNT);
        out.collect(value);
      } else {
        metrics.inc(MetricKeys.DEDUP_DUPLICATE_COUNT);
        log.debug("[Deduplicator] Dropped duplicate for key: {}", ctx.getCurrentKey());
      }
    }
  }
}
