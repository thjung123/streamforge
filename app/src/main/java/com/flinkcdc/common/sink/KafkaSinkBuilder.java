package com.flinkcdc.common.sink;

import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.utils.JsonUtils;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class KafkaSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream) {
        KafkaSink<CdcEnvelop> sink = KafkaSink.<CdcEnvelop>builder()
                .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<CdcEnvelop>builder()
                                .setTopic(require(KAFKA_SINK_TOPIC))
                                .setValueSerializationSchema(
                                        envelop -> JsonUtils.toJson(envelop).getBytes()
                                )
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return stream.sinkTo(sink).name("KafkaSink");
    }
}
