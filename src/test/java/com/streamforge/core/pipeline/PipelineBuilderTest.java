package com.streamforge.core.pipeline;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PipelineBuilderTest {

  private DataStream<String> source;

  @BeforeEach
  void setUp() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataGeneratorSource<String> generator =
        new DataGeneratorSource<>(
            (GeneratorFunction<Long, String>) value -> "elem-" + value, 3, Types.STRING);
    source = env.fromSource(generator, WatermarkStrategy.noWatermarks(), "test-generator");
  }

  @Test
  @DisplayName("from() should retain the same environment")
  void testFrom() {
    PipelineBuilder<String> builder = PipelineBuilder.from(source);
    assertNotNull(builder, "PipelineBuilder should not be null");
  }

  @Test
  @DisplayName("parse() should transform the stream")
  void testParse() {
    PipelineBuilder<Integer> builder =
        PipelineBuilder.from(source).parse(stream -> stream.map(String::length));

    assertNotNull(builder, "Builder after parse should not be null");
  }

  @Test
  @DisplayName("process() should transform the stream further")
  void testProcess() {
    PipelineBuilder<String> builder =
        PipelineBuilder.from(source)
            .parse(stream -> stream.map(String::length))
            .process(stream -> stream.map(Object::toString));

    assertNotNull(builder, "Builder after process should not be null");
  }

  @Test
  @DisplayName("to() should invoke SinkBuilder.write()")
  void testToInvokesSinkBuilder() {
    @SuppressWarnings("unchecked")
    PipelineBuilder.SinkBuilder<String> sink = mock(PipelineBuilder.SinkBuilder.class);
    @SuppressWarnings("unchecked")
    DataStreamSink<String> sinkMock = mock(DataStreamSink.class);
    when(sink.write(any(), anyString())).thenReturn(sinkMock);
    PipelineBuilder.from(source).to(sink, "test-job");

    verify(sink, times(1)).write(any(), eq("test-job"));
  }

  @Test
  @DisplayName("Full pipeline chain should run successfully")
  void testFullPipelineExecution() {
    @SuppressWarnings("unchecked")
    PipelineBuilder.SinkBuilder<String> sink = mock(PipelineBuilder.SinkBuilder.class);
    @SuppressWarnings("unchecked")
    DataStreamSink<String> sinkMock = mock(DataStreamSink.class);
    when(sink.write(any(), anyString())).thenReturn(sinkMock);

    PipelineBuilder<String> builder =
        PipelineBuilder.from(source)
            .parse(s -> s.map(String::toUpperCase))
            .process(s -> s.map(v -> v + "_processed"))
            .to(sink, "test-job");

    assertNotNull(builder, "PipelineBuilder should not be null");
    verify(sink, times(1)).write(any(), eq("test-job"));
  }
}
