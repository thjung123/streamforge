package com.flinkcdc.common.sink;

import com.flinkcdc.common.model.CdcEnvelop;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaSinkBuilderTest {

    @Test
    void testWriteAddsKafkaSink() {
        // given
        @SuppressWarnings("unchecked")
        DataStream<CdcEnvelop> mockStream = mock(DataStream.class);
        @SuppressWarnings("unchecked")
        DataStreamSink<CdcEnvelop> mockSink = mock(DataStreamSink.class);

        when(mockStream.sinkTo(any(Sink.class))).thenReturn(mockSink);
        when(mockSink.name(anyString())).thenReturn(mockSink);

        KafkaSinkBuilder builder = new KafkaSinkBuilder();

        // when
        DataStreamSink<CdcEnvelop> result = builder.write(mockStream);

        // then
        assertThat(result).isNotNull();
        verify(mockStream, times(1)).sinkTo(any(Sink.class));
        verify(mockSink, times(1)).name("KafkaSink");
    }
}
