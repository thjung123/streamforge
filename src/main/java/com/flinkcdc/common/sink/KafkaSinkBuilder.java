package com.flinkcdc.common.sink;

import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.utils.JsonUtils;
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

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream) {
        KafkaSink<CdcEnvelop> sink = KafkaSink.<CdcEnvelop>builder()
                .setBootstrapServers(require(KAFKA_BOOTSTRAP_SERVERS))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<CdcEnvelop>builder()
                                .setTopic(require(KAFKA_SINK_TOPIC))
                                .setValueSerializationSchema(
                                        envelop -> {
                                            try {
                                                return JsonUtils.toJson(envelop).getBytes();
                                            } catch (Exception e) {
                                                log.error("Kafka sink serialization failed: {}", envelop, e);
                                                DlqEvent dlqEvent = DlqEvent.of(
                                                        "SINK_ERROR",
                                                        e.getMessage(),
                                                        "kafka-sink",
                                                        envelop != null ? envelop.toJson() : null,
                                                        e
                                                );
                                                DLQPublisher.getInstance().publish(dlqEvent);
                                                return "{}".getBytes();
                                            }
                                        }).build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return stream.sinkTo(sink).name("KafkaSink");
    }
}
