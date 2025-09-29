package com.flinkcdc.common.source;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaSourceBuilderTest {

    @Test
    void testBuildCreatesKafkaSource() {
        // given
        @SuppressWarnings("unchecked")
        StreamExecutionEnvironment mockEnv = mock(StreamExecutionEnvironment.class);
        @SuppressWarnings("unchecked")
        DataStreamSource<String> mockDataStreamSource = mock(DataStreamSource.class);

        when(mockEnv.fromSource(any(Source.class), any(), eq("KafkaSource")))
                .thenReturn(mockDataStreamSource);

        KafkaSourceBuilder builder = new KafkaSourceBuilder();

        // when
        var result = builder.build(mockEnv);

        // then
        assertThat(result).isNotNull();
        verify(mockEnv, times(1)).fromSource(any(Source.class), any(), eq("KafkaSource"));
    }
}
