package com.flinkcdc.common.sink;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ErrorCodes.SINK_ERROR;
import static com.flinkcdc.common.config.ScopedConfig.*;
import static com.mongodb.client.model.Filters.eq;


public class MongoSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

    public static final String OPERATOR_NAME = "MongoSink";

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream, String jobName) {
        return stream
                .addSink(new MongoSinkFunction(jobName))
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

        // for unit tests
        public MongoSinkFunction(String jobName, MongoCollection<Document> collection) {
            this.jobName = jobName;
            this.collection = collection;
        }

        @Override
        public void open(Configuration parameters) {
            metrics = new Metrics(getRuntimeContext(), jobName, MetricKeys.MONGO);
            DLQPublisher.getInstance().initMetrics(getRuntimeContext(), jobName);

            if (collection == null) {
                client = MongoClients.create(require(MONGO_URI));
                MongoDatabase db = client.getDatabase(require(MONGO_DB));
                collection = db.getCollection(require(MONGO_COLLECTION));
            }

            log.info("[MongoSink] Initialized for job={} collection={}", jobName, require(MONGO_COLLECTION));
        }

        @Override
        public void invoke(CdcEnvelop value, Context context) {
            try {
                String op = value.getOperation();
                Object pkValue = value.getPayloadAsMap() != null && value.getPrimaryKey() != null
                        ? value.getPayloadAsMap().get(value.getPrimaryKey())
                        : null;

                if (pkValue == null) {
                    log.warn("[MongoSink] Missing PK value: {}", value);
                    metrics.inc(MetricKeys.SINK_ERROR_COUNT);
                    return;
                }

                Document doc = Document.parse(value.getPayloadJson());
                doc.put("_id", pkValue);

                if ("DELETE".equalsIgnoreCase(op)) {
                    collection.deleteOne(eq("_id", pkValue));
                } else {
                    collection.replaceOne(eq("_id", pkValue), doc, new ReplaceOptions().upsert(true));
                }

                metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);
            } catch (Exception e) {
                metrics.inc(MetricKeys.SINK_ERROR_COUNT);
                log.error("[MongoSink] Sink error", e);
                DLQPublisher.getInstance().publish(
                        DlqEvent.of(SINK_ERROR, e.getMessage(), OPERATOR_NAME, value != null ? value.toJson() : null, e)
                );
            }
        }


        private void performUpsert(CdcEnvelop value, Object pkValue) {
            try {
                Document doc = Document.parse(value.getPayloadJson());
                doc.put("_id", pkValue);

                var result = collection.replaceOne(
                        new Document("_id", pkValue),
                        doc,
                        new ReplaceOptions().upsert(true)
                );

                log.info("[MongoSink] Upsert payload={}, pkField={}, pkValue={}, doc={}",
                        value.getPayloadJson(),
                        value.getPrimaryKey(),
                        pkValue,
                        doc.toJson());

                metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);

            } catch (Exception e) {
                handleSinkError(value, e);
            }
        }

        private void performDelete(Object pkValue) {
            try {
                var result = collection.deleteOne(new Document("_id", pkValue));

                log.debug("[MongoSink] Delete op success: pk={}, deletedCount={}",
                        pkValue, result.getDeletedCount());

                metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);

            } catch (Exception e) {
                handleSinkError(null, e);
            }
        }

        private void handleSinkError(CdcEnvelop value, Exception e) {
            metrics.inc(MetricKeys.SINK_ERROR_COUNT);

            log.error("[MongoSink] Sink error for job={} value={}", jobName, value, e);

            DlqEvent dlqEvent = DlqEvent.of(
                    SINK_ERROR,
                    e.getMessage(),
                    OPERATOR_NAME,
                    value != null ? value.toJson() : null,
                    e
            );

            DLQPublisher.getInstance().publish(dlqEvent);
        }

        @Override
        public void close() {
            if (client != null) {
                try {
                    client.close();
                    log.info("[MongoSink] MongoClient closed for job={}", jobName);
                } catch (Exception e) {
                    log.warn("[MongoSink] Failed to close MongoClient for job={}", jobName, e);
                }
            }
        }
    }
}
