package com.flinkcdc.common.source;

import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class KafkaSourceBuilder implements PipelineBuilder.SourceBuilder<String> {

    @Override
    public DataStream<String> build(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
                .setTopics(require(KAFKA_SOURCE_TOPIC))
                .setGroupId("cdc-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource");
    }
}
