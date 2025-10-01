package com.flinkcdc.common.sink;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.utils.JsonUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class KafkaSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSinkBuilder.class);
    public static final String OPERATOR_NAME = "KafkaSink";

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream, String jobName) {
        return stream
                .map(new MetricsMapFunction(jobName))
                .sinkTo(KafkaSink.<CdcEnvelop>builder()
                        .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
                        .setRecordSerializer(KafkaRecordSerializationSchema.<CdcEnvelop>builder()
                                .setTopic(require(KAFKA_SINK_TOPIC))
                                .setValueSerializationSchema(envelop -> JsonUtils.toJson(envelop).getBytes())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name(OPERATOR_NAME);
    }

    static class MetricsMapFunction extends RichMapFunction<CdcEnvelop, CdcEnvelop> {

        private transient Metrics metrics;
        private final String jobName;


        public MetricsMapFunction(String jobName) {
            this.jobName = jobName;
        }

        @Override
        public void open(Configuration parameters) {
            this.metrics = new Metrics(getRuntimeContext(), jobName, MetricKeys.KAFKA);
        }

        @Override
        public CdcEnvelop map(CdcEnvelop envelop) {
            try {
                metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);
                return envelop;
            } catch (Exception e) {
                metrics.inc(MetricKeys.SINK_ERROR_COUNT);
                throw e;
            }
        }
    }
}
