package com.streamforge.pattern.enrich;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicJoiner<T, R> implements PipelineBuilder.JoinPattern<T, R> {

  public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL_OUTER
  }

  @FunctionalInterface
  public interface KeyExtractor<V> extends Serializable {
    String extract(V value);
  }

  @FunctionalInterface
  public interface JoinFunction<T, R> extends Serializable {
    T join(T left, R right);
  }

  @FunctionalInterface
  public interface RightEmitFunction<R, T> extends Serializable {
    T convert(R right);
  }

  static class BufferedEvent<V> implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    private final V event;
    private final long arrivalTime;
    private boolean matched;

    BufferedEvent(V event, long arrivalTime) {
      this.event = event;
      this.arrivalTime = arrivalTime;
      this.matched = false;
    }

    V getEvent() {
      return event;
    }

    long getArrivalTime() {
      return arrivalTime;
    }

    boolean isUnmatched() {
      return !matched;
    }

    void markMatched() {
      this.matched = true;
    }
  }

  private final KeyExtractor<T> leftKeyExtractor;
  private final KeyExtractor<R> rightKeyExtractor;
  private final JoinFunction<T, R> joinFunction;
  private final JoinType joinType;
  private final Duration ttl;
  private final RightEmitFunction<R, T> rightEmitFunction;

  private DynamicJoiner(
      KeyExtractor<T> leftKeyExtractor,
      KeyExtractor<R> rightKeyExtractor,
      JoinFunction<T, R> joinFunction,
      JoinType joinType,
      Duration ttl,
      RightEmitFunction<R, T> rightEmitFunction) {
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.joinFunction = joinFunction;
    this.joinType = joinType;
    this.ttl = ttl;
    this.rightEmitFunction = rightEmitFunction;
  }

  public static <T, R> Builder<T, R> builder() {
    return new Builder<>();
  }

  @Override
  public DataStream<T> join(DataStream<T> mainStream, DataStream<R> referenceStream) {
    return mainStream
        .connect(referenceStream)
        .keyBy(leftKeyExtractor::extract, rightKeyExtractor::extract)
        .process(new DynamicJoinFunction<>(joinFunction, joinType, ttl, rightEmitFunction))
        .returns(mainStream.getType())
        .name("DynamicJoiner");
  }

  public static final class Builder<T, R> {

    private KeyExtractor<T> leftKeyExtractor;
    private KeyExtractor<R> rightKeyExtractor;
    private JoinFunction<T, R> joinFunction;
    private JoinType joinType = JoinType.INNER;
    private Duration ttl = Duration.ofMinutes(10);
    private RightEmitFunction<R, T> rightEmitFunction;

    private Builder() {}

    public Builder<T, R> leftKeyExtractor(KeyExtractor<T> leftKeyExtractor) {
      this.leftKeyExtractor = leftKeyExtractor;
      return this;
    }

    public Builder<T, R> rightKeyExtractor(KeyExtractor<R> rightKeyExtractor) {
      this.rightKeyExtractor = rightKeyExtractor;
      return this;
    }

    public Builder<T, R> joinFunction(JoinFunction<T, R> joinFunction) {
      this.joinFunction = joinFunction;
      return this;
    }

    public Builder<T, R> joinType(JoinType joinType) {
      this.joinType = joinType;
      return this;
    }

    public Builder<T, R> ttl(Duration ttl) {
      this.ttl = ttl;
      return this;
    }

    public Builder<T, R> rightEmitFunction(RightEmitFunction<R, T> rightEmitFunction) {
      this.rightEmitFunction = rightEmitFunction;
      return this;
    }

    public DynamicJoiner<T, R> build() {
      if (leftKeyExtractor == null) {
        throw new IllegalArgumentException("leftKeyExtractor must not be null");
      }
      if (rightKeyExtractor == null) {
        throw new IllegalArgumentException("rightKeyExtractor must not be null");
      }
      if (joinFunction == null) {
        throw new IllegalArgumentException("joinFunction must not be null");
      }
      if (joinType == null) {
        throw new IllegalArgumentException("joinType must not be null");
      }
      if (ttl == null) {
        throw new IllegalArgumentException("ttl must not be null");
      }
      if ((joinType == JoinType.RIGHT || joinType == JoinType.FULL_OUTER)
          && rightEmitFunction == null) {
        throw new IllegalArgumentException(
            "rightEmitFunction must not be null for RIGHT/FULL_OUTER join");
      }
      return new DynamicJoiner<>(
          leftKeyExtractor, rightKeyExtractor, joinFunction, joinType, ttl, rightEmitFunction);
    }
  }

  static class DynamicJoinFunction<T, R>
      extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, T, R, T> {

    private static final Logger log = LoggerFactory.getLogger(DynamicJoiner.class);

    private final JoinFunction<T, R> joinFunction;
    private final JoinType joinType;
    private final Duration ttl;
    private final RightEmitFunction<R, T> rightEmitFunction;

    private transient MapState<Long, BufferedEvent<T>> leftBuffer;
    private transient MapState<Long, BufferedEvent<R>> rightBuffer;
    private transient ValueState<Long> seqCounter;
    private transient Metrics metrics;

    DynamicJoinFunction(
        JoinFunction<T, R> joinFunction,
        JoinType joinType,
        Duration ttl,
        RightEmitFunction<R, T> rightEmitFunction) {
      this.joinFunction = joinFunction;
      this.joinType = joinType;
      this.ttl = ttl;
      this.rightEmitFunction = rightEmitFunction;
    }

    @Override
    public void open(Configuration parameters) {
      StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl).build();

      MapStateDescriptor<Long, BufferedEvent<T>> leftDesc =
          new MapStateDescriptor<>(
              "left-buffer",
              TypeInformation.of(Long.class),
              TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<>() {}));
      leftDesc.enableTimeToLive(ttlConfig);
      this.leftBuffer = getRuntimeContext().getMapState(leftDesc);

      MapStateDescriptor<Long, BufferedEvent<R>> rightDesc =
          new MapStateDescriptor<>(
              "right-buffer",
              TypeInformation.of(Long.class),
              TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<>() {}));
      rightDesc.enableTimeToLive(ttlConfig);
      this.rightBuffer = getRuntimeContext().getMapState(rightDesc);

      ValueStateDescriptor<Long> seqDesc = new ValueStateDescriptor<>("seq-counter", Long.class);
      this.seqCounter = getRuntimeContext().getState(seqDesc);

      this.metrics = new Metrics(getRuntimeContext(), "dynamic_joiner", "DynamicJoiner");
    }

    private long nextSeq() throws Exception {
      Long current = seqCounter.value();
      long next = (current == null) ? 0L : current + 1;
      seqCounter.update(next);
      return next;
    }

    @Override
    public void processElement1(T left, Context ctx, Collector<T> out) throws Exception {
      String key = ctx.getCurrentKey();
      if (key == null) {
        log.warn("[DynamicJoiner] Null key from left event, skipping");
        return;
      }

      long now = ctx.timerService().currentProcessingTime();

      for (Map.Entry<Long, BufferedEvent<R>> entry : rightBuffer.entries()) {
        BufferedEvent<R> buffered = entry.getValue();
        out.collect(joinFunction.join(left, buffered.getEvent()));
        metrics.inc(MetricKeys.DYNAMIC_JOINER_MATCH_COUNT);
        buffered.markMatched();
        rightBuffer.put(entry.getKey(), buffered);
      }

      BufferedEvent<T> leftEvent = new BufferedEvent<>(left, now);
      if (rightBuffer.entries().iterator().hasNext()) {
        leftEvent.markMatched();
      }
      long seq = nextSeq();
      leftBuffer.put(seq, leftEvent);

      if (joinType == JoinType.LEFT || joinType == JoinType.FULL_OUTER) {
        ctx.timerService().registerProcessingTimeTimer(now + ttl.toMillis());
      }
    }

    @Override
    public void processElement2(R right, Context ctx, Collector<T> out) throws Exception {
      String key = ctx.getCurrentKey();
      if (key == null) {
        log.warn("[DynamicJoiner] Null key from right event, skipping");
        return;
      }

      long now = ctx.timerService().currentProcessingTime();

      for (Map.Entry<Long, BufferedEvent<T>> entry : leftBuffer.entries()) {
        BufferedEvent<T> buffered = entry.getValue();
        out.collect(joinFunction.join(buffered.getEvent(), right));
        metrics.inc(MetricKeys.DYNAMIC_JOINER_MATCH_COUNT);
        buffered.markMatched();
        leftBuffer.put(entry.getKey(), buffered);
      }

      BufferedEvent<R> rightEvent = new BufferedEvent<>(right, now);
      if (leftBuffer.entries().iterator().hasNext()) {
        rightEvent.markMatched();
      }
      long seq = nextSeq();
      rightBuffer.put(seq, rightEvent);

      if (joinType == JoinType.RIGHT || joinType == JoinType.FULL_OUTER) {
        ctx.timerService().registerProcessingTimeTimer(now + ttl.toMillis());
      }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
      if (joinType == JoinType.LEFT || joinType == JoinType.FULL_OUTER) {
        var leftIter = leftBuffer.entries().iterator();
        while (leftIter.hasNext()) {
          Map.Entry<Long, BufferedEvent<T>> entry = leftIter.next();
          BufferedEvent<T> buffered = entry.getValue();
          if (buffered.getArrivalTime() + ttl.toMillis() <= timestamp && buffered.isUnmatched()) {
            out.collect(buffered.getEvent());
            metrics.inc(MetricKeys.DYNAMIC_JOINER_LEFT_PASS_COUNT);
            metrics.inc(MetricKeys.DYNAMIC_JOINER_EXPIRE_COUNT);
            leftIter.remove();
          }
        }
      }

      if (joinType == JoinType.RIGHT || joinType == JoinType.FULL_OUTER) {
        var rightIter = rightBuffer.entries().iterator();
        while (rightIter.hasNext()) {
          Map.Entry<Long, BufferedEvent<R>> entry = rightIter.next();
          BufferedEvent<R> buffered = entry.getValue();
          if (buffered.getArrivalTime() + ttl.toMillis() <= timestamp && buffered.isUnmatched()) {
            out.collect(rightEmitFunction.convert(buffered.getEvent()));
            metrics.inc(MetricKeys.DYNAMIC_JOINER_RIGHT_PASS_COUNT);
            metrics.inc(MetricKeys.DYNAMIC_JOINER_EXPIRE_COUNT);
            rightIter.remove();
          }
        }
      }
    }
  }
}
