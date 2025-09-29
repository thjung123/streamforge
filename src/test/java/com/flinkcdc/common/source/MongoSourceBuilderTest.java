package com.flinkcdc.common.source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

class MongoSourceBuilderTest {

    @Test
    void testBuildCreatesDataStream() {
        // given
        StreamExecutionEnvironment mockEnv = mock(StreamExecutionEnvironment.class);
        DataStreamSource<Document> mockSource = mock(DataStreamSource.class);

        doReturn(mockSource)
                .when(mockEnv)
                .fromSource(any(), any(), any());

        MongoSourceBuilder builder = new MongoSourceBuilder();

        // when
        var result = builder.build(mockEnv);

        // then
        assertThat(result).isNotNull();
        verify(mockEnv, times(1)).fromSource(any(), any(), eq("MongoDBSource"));
    }
}
