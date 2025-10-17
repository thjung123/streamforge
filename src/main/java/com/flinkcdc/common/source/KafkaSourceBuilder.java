package com.flinkcdc.common.source;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class KafkaSourceBuilder implements PipelineBuilder.SourceBuilder<String> {

    public static final String OPERATOR_NAME = "KafkaSource";

    @Override
    public DataStream<String> build(StreamExecutionEnvironment env, String jobName) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
                .setTopics(require(CDC_TOPIC))
                .setGroupId("cdc-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), jobName + "-" + OPERATOR_NAME)
                .map(new MetricCountingMap(jobName))
                .name(OPERATOR_NAME);
    }

    static class MetricCountingMap extends RichMapFunction<String, String> {
        private final String jobName;
        private transient Metrics metrics;

        public MetricCountingMap(String jobName) {
            this.jobName = jobName;
        }

        @Override
        public void open(Configuration parameters) {
            metrics = new Metrics(getRuntimeContext(), jobName, MetricKeys.KAFKA);
        }

        @Override
        public String map(String value) {
            try {
                metrics.inc(MetricKeys.SOURCE_READ_COUNT);
                return value;
            } catch (Exception e) {
                metrics.inc(MetricKeys.SOURCE_ERROR_COUNT);
                throw e;
            }
        }
    }
}
