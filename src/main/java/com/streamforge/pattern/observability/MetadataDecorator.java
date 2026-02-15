package com.streamforge.pattern.observability;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.time.Instant;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

public class MetadataDecorator<T> implements PipelineBuilder.StreamPattern<T> {

  private final MetadataAccessor<T> metadataAccessor;
  private final String stageName;

  public MetadataDecorator(MetadataAccessor<T> metadataAccessor, String stageName) {
    this.metadataAccessor = metadataAccessor;
    this.stageName = stageName;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream.map(new DecoratorFunction<>(metadataAccessor, stageName, name())).name(name());
  }

  static class DecoratorFunction<T> extends RichMapFunction<T, T> {

    private final MetadataAccessor<T> metadataAccessor;
    private final String stageName;
    private final String operatorName;
    private transient Metrics metrics;
    private transient String taskName;
    private transient int subtaskIndex;

    DecoratorFunction(MetadataAccessor<T> metadataAccessor, String stageName, String operatorName) {
      this.metadataAccessor = metadataAccessor;
      this.stageName = stageName;
      this.operatorName = operatorName;
    }

    DecoratorFunction(
        MetadataAccessor<T> metadataAccessor,
        String stageName,
        String operatorName,
        Metrics metrics,
        String taskName,
        int subtaskIndex) {
      this.metadataAccessor = metadataAccessor;
      this.stageName = stageName;
      this.operatorName = operatorName;
      this.metrics = metrics;
      this.taskName = taskName;
      this.subtaskIndex = subtaskIndex;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "decorator", operatorName);
      this.taskName = getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks();
      this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public T map(T value) {
      Map<String, String> metadata = metadataAccessor.getMetadata(value);
      String prefix = "stage." + stageName;
      metadata.put(prefix + ".taskName", taskName);
      metadata.put(prefix + ".subtaskIndex", String.valueOf(subtaskIndex));
      metadata.put(prefix + ".processedAt", Instant.now().toString());

      metrics.inc(MetricKeys.DECORATOR_EVENT_COUNT);
      return value;
    }
  }
}
