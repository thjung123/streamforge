package com.streamforge.core.pipeline;

import lombok.Getter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PipelineBuilder<T> {

  private final StreamExecutionEnvironment env;
  @Getter private final DataStream<T> stream;

  private PipelineBuilder(StreamExecutionEnvironment env, DataStream<T> stream) {
    this.env = env;
    this.stream = stream;
  }

  public static <T> PipelineBuilder<T> from(DataStream<T> source) {
    return new PipelineBuilder<>(source.getExecutionEnvironment(), source);
  }

  public <R> PipelineBuilder<R> parse(ParserFunction<T, R> parser) {
    DataStream<R> parsed = parser.parse(this.stream);
    return new PipelineBuilder<>(this.env, parsed);
  }

  public <R> PipelineBuilder<R> process(ProcessorFunction<T, R> processor) {
    DataStream<R> processed = processor.process(this.stream);
    return new PipelineBuilder<>(this.env, processed);
  }

  public <R> PipelineBuilder<T> enrich(DataStream<R> referenceStream, JoinPattern<T, R> pattern) {
    DataStream<T> enriched = pattern.join(this.stream, referenceStream);
    return new PipelineBuilder<>(this.env, enriched);
  }

  public PipelineBuilder<T> apply(StreamPattern<T> pattern) {
    DataStream<T> transformed = pattern.apply(this.stream);
    return new PipelineBuilder<>(this.env, transformed);
  }

  public PipelineBuilder<T> to(SinkBuilder<T> sink, String jobName) {
    sink.write(stream, jobName);
    return this;
  }

  @FunctionalInterface
  public interface ParserFunction<I, O> {
    DataStream<O> parse(DataStream<I> input);
  }

  @FunctionalInterface
  public interface ProcessorFunction<I, O> {
    DataStream<O> process(DataStream<I> input);
  }

  @FunctionalInterface
  public interface SinkBuilder<T> {
    DataStreamSink<T> write(DataStream<T> stream, String jobName);
  }

  public interface SourceBuilder<T> {
    DataStream<T> build(StreamExecutionEnvironment env, String jobName);
  }

  /**
   * Common interface for all stream patterns. Accepts a DataStream and returns a transformed
   * DataStream.
   */
  public interface StreamPattern<T> {
    DataStream<T> apply(DataStream<T> stream);

    default String name() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Interface for two-stream join patterns. Joins a main stream with a reference stream and returns
   * the enriched main stream.
   */
  public interface JoinPattern<T, R> {
    DataStream<T> join(DataStream<T> mainStream, DataStream<R> referenceStream);
  }
}
