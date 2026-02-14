package com.streamforge.pattern.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.streamforge.core.pipeline.PipelineBuilder;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FilterInterceptorTest {

  @Test
  @DisplayName("All elements should pass when predicate always returns true")
  void allPassWhenPredicateAlwaysTrue() throws Exception {
    var filter = new FilterInterceptor.MetricFilter<Long>(value -> true, "test");
    var operator = new StreamFilter<>(filter);

    try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      for (long i = 0; i < 5; i++) {
        harness.processElement(new StreamRecord<>(i, i));
      }

      List<Long> output =
          harness.extractOutputStreamRecords().stream().map(r -> (Long) r.getValue()).toList();

      assertThat(output).hasSize(5);
    }
  }

  @Test
  @DisplayName("Elements should be filtered when predicate rejects some")
  void partialFilterWhenPredicateRejectsSome() throws Exception {
    var filter = new FilterInterceptor.MetricFilter<Long>(value -> value % 2 == 0, "test");
    var operator = new StreamFilter<>(filter);

    try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      for (long i = 0; i < 10; i++) {
        harness.processElement(new StreamRecord<>(i, i));
      }

      List<Long> output =
          harness.extractOutputStreamRecords().stream().map(r -> (Long) r.getValue()).toList();

      assertThat(output).hasSize(5).allMatch(v -> v % 2 == 0);
    }
  }

  @Test
  @DisplayName("FilterInterceptor should work within PipelineBuilder.apply() chain")
  void worksInPipelineBuilderChain() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataGeneratorSource<Long> generator =
        new DataGeneratorSource<>((GeneratorFunction<Long, Long>) value -> value, 6, Types.LONG);
    DataStream<Long> source =
        env.fromSource(generator, WatermarkStrategy.noWatermarks(), "test-source");

    @SuppressWarnings("unchecked")
    PipelineBuilder.SinkBuilder<Long> sink = mock(PipelineBuilder.SinkBuilder.class);
    @SuppressWarnings("unchecked")
    DataStreamSink<Long> sinkMock = mock(DataStreamSink.class);
    when(sink.write(any(), anyString())).thenReturn(sinkMock);

    FilterInterceptor<Long> filter = new FilterInterceptor<>(value -> value < 3);

    PipelineBuilder<Long> builder = PipelineBuilder.from(source).apply(filter).to(sink, "test-job");

    assertThat(builder).isNotNull();
    verify(sink, times(1)).write(any(), eq("test-job"));
  }
}
