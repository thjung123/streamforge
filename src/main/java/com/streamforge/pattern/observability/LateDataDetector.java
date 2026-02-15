package com.streamforge.pattern.observability;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateDataDetector<T> implements PipelineBuilder.StreamPattern<T> {

  public static final OutputTag<String> ALERT_TAG = new OutputTag<>("late-data-alert") {};

  private final TimestampExtractor<T> eventTimeExtractor;
  private final Duration allowedLateness;

  public LateDataDetector(TimestampExtractor<T> eventTimeExtractor, Duration allowedLateness) {
    this.eventTimeExtractor = eventTimeExtractor;
    this.allowedLateness = allowedLateness;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .process(new LateDataFunction<>(eventTimeExtractor, allowedLateness, name()))
        .name(name());
  }

  static class LateDataFunction<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(LateDataDetector.class);

    private final TimestampExtractor<T> eventTimeExtractor;
    private final Duration allowedLateness;
    private final String operatorName;
    private transient Metrics metrics;

    LateDataFunction(
        TimestampExtractor<T> eventTimeExtractor, Duration allowedLateness, String operatorName) {
      this.eventTimeExtractor = eventTimeExtractor;
      this.allowedLateness = allowedLateness;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "late", operatorName);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      Instant eventTime = eventTimeExtractor.extract(value);
      if (eventTime != null) {
        long watermark = ctx.timerService().currentWatermark();
        long eventMs = eventTime.toEpochMilli();

        if (watermark > Long.MIN_VALUE && eventMs + allowedLateness.toMillis() < watermark) {
          metrics.inc(MetricKeys.LATE_EVENT_COUNT);
          log.warn(
              "[LateDataDetector] Late event: eventTime={}, watermark={}ms, allowedLateness={}ms",
              eventTime,
              watermark,
              allowedLateness.toMillis());
          ctx.output(
              ALERT_TAG, "Late event: eventTime=" + eventTime + ", watermark=" + watermark + "ms");
        }
      }
      out.collect(value);
    }
  }
}
