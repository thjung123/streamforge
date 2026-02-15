package com.streamforge.pattern.observability;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.time.Duration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowDisruptionDetector<T> implements PipelineBuilder.StreamPattern<T> {

  public static final OutputTag<String> ALERT_TAG = new OutputTag<>("flow-disruption-alert") {};

  private final KeySelector<T, String> keySelector;
  private final Duration timeout;

  public FlowDisruptionDetector(KeySelector<T, String> keySelector, Duration timeout) {
    this.keySelector = keySelector;
    this.timeout = timeout;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .keyBy(keySelector)
        .process(new DisruptionFunction<>(timeout, name()))
        .returns(stream.getType())
        .name(name());
  }

  static class DisruptionFunction<T> extends KeyedProcessFunction<String, T, T> {

    private static final Logger log = LoggerFactory.getLogger(FlowDisruptionDetector.class);

    private final Duration timeout;
    private final String operatorName;
    private transient ValueState<Boolean> disrupted;
    private transient ValueState<Long> timerTimestamp;
    private transient Metrics metrics;

    DisruptionFunction(Duration timeout, String operatorName) {
      this.timeout = timeout;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.disrupted =
          getRuntimeContext().getState(new ValueStateDescriptor<>("disrupted", Boolean.class));
      this.timerTimestamp =
          getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
      this.metrics = new Metrics(getRuntimeContext(), "flow", operatorName);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
      if (Boolean.TRUE.equals(disrupted.value())) {
        disrupted.update(false);
        metrics.inc(MetricKeys.FLOW_RECOVERY_COUNT);
        log.info("[FlowDisruptionDetector] Flow recovered for key: {}", ctx.getCurrentKey());
        ctx.output(ALERT_TAG, "Flow recovered for key: " + ctx.getCurrentKey());
      }

      Long existingTimer = timerTimestamp.value();
      if (existingTimer != null) {
        ctx.timerService().deleteProcessingTimeTimer(existingTimer);
      }

      long nextTimer = ctx.timerService().currentProcessingTime() + timeout.toMillis();
      ctx.timerService().registerProcessingTimeTimer(nextTimer);
      timerTimestamp.update(nextTimer);

      out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
      disrupted.update(true);
      timerTimestamp.clear();
      metrics.inc(MetricKeys.FLOW_DISRUPTION_COUNT);
      log.warn(
          "[FlowDisruptionDetector] No events for {}ms, key: {}",
          timeout.toMillis(),
          ctx.getCurrentKey());
      ctx.output(
          ALERT_TAG, "No events for " + timeout.toMillis() + "ms, key: " + ctx.getCurrentKey());
    }
  }
}
