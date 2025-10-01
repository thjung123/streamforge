package com.flinkcdc.common.sink;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.utils.JsonUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class MongoSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

    public static final String OPERATOR_NAME = "MongoSink";

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream, String jobName) {
        return stream.addSink(new MongoSinkFunction(jobName))
                .name(OPERATOR_NAME);
    }

    static class MongoSinkFunction extends RichSinkFunction<CdcEnvelop> {

        private static final Logger log = LoggerFactory.getLogger(MongoSinkFunction.class);

        private transient MongoClient client;
        private transient MongoCollection<Document> collection;
        private transient Metrics metrics;

        private final String jobName;

        public MongoSinkFunction(String jobName) {
            this.jobName = jobName;
        }

        public MongoSinkFunction(String jobName, MongoCollection<Document> collection) {
            this.jobName = jobName;
            this.collection = collection;
        }

        @Override
        public void open(Configuration parameters) {
            metrics = new Metrics(getRuntimeContext(), jobName, MetricKeys.MONGO);
            DLQPublisher.getInstance().initMetrics(getRuntimeContext(), jobName);

            client = MongoClients.create(require(MONGO_URI));
            MongoDatabase db = client.getDatabase(require(MONGO_DB));
            collection = db.getCollection(require(MONGO_COLLECTION));
        }

        @Override
        public void invoke(CdcEnvelop value, Context context) {
            try {
                String json = JsonUtils.toJson(value);
                Document doc = Document.parse(json);
                collection.insertOne(doc);

                metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);

            } catch (Exception e) {
                metrics.inc(MetricKeys.SINK_ERROR_COUNT);
                log.error("Mongo sink failed for envelop: {}", value, e);

                DlqEvent dlqEvent = DlqEvent.of(
                        "SINK_ERROR",
                        e.getMessage(),
                        OPERATOR_NAME,
                        value != null ? value.toJson() : null,
                        e
                );
                DLQPublisher.getInstance().publish(dlqEvent);
            }
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
