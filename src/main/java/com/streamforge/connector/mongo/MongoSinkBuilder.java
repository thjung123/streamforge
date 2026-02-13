package com.streamforge.connector.mongo;

import static com.mongodb.client.model.Filters.eq;
import static com.streamforge.connector.mongo.MongoConfigKeys.*;
import static com.streamforge.core.config.ErrorCodes.SINK_ERROR;
import static com.streamforge.core.config.ScopedConfig.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

  public static final String OPERATOR_NAME = "MongoSink";

  @Override
  public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream, String jobName) {
    return stream.sinkTo(new MongoSink(jobName)).name(OPERATOR_NAME);
  }

  static class MongoSink implements Sink<CdcEnvelop> {
    private final String jobName;

    public MongoSink(String jobName) {
      this.jobName = jobName;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<CdcEnvelop> createWriter(InitContext context) {
      return new MongoSinkWriter(jobName, context);
    }
  }

  static class MongoSinkWriter implements SinkWriter<CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(MongoSinkWriter.class);

    private final MongoClient client;
    private final MongoCollection<Document> collection;
    private final Metrics metrics;
    private final String jobName;

    @SuppressWarnings("deprecation")
    MongoSinkWriter(String jobName, Sink.InitContext context) {
      this.jobName = jobName;
      this.metrics = new Metrics(context.metricGroup(), jobName, MetricKeys.MONGO);
      this.client = MongoClients.create(require(MONGO_URI));
      MongoDatabase db = client.getDatabase(require(MONGO_DB));
      this.collection = db.getCollection(require(MONGO_COLLECTION));
      log.info(
          "[MongoSink] Initialized for job={} collection={}", jobName, require(MONGO_COLLECTION));
    }

    // for unit tests
    MongoSinkWriter(String jobName, MongoCollection<Document> collection) {
      this.jobName = jobName;
      this.client = null;
      this.collection = collection;
      this.metrics = null;
    }

    @Override
    public void write(CdcEnvelop value, Context context) {
      try {
        String op = value.getOperation();
        Object pkValue =
            value.getPayloadAsMap() != null && value.getPrimaryKey() != null
                ? value.getPayloadAsMap().get(value.getPrimaryKey())
                : null;

        if (pkValue == null) {
          log.warn("[MongoSink] Missing PK value: {}", value);
          if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
          return;
        }

        Document doc = Document.parse(value.getPayloadJson());
        doc.put("_id", pkValue);

        if ("DELETE".equalsIgnoreCase(op)) {
          collection.deleteOne(eq("_id", pkValue));
        } else {
          collection.replaceOne(eq("_id", pkValue), doc, new ReplaceOptions().upsert(true));
        }

        if (metrics != null) metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);
      } catch (Exception e) {
        if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
        log.error("[MongoSink] Sink error", e);
        DLQPublisher.getInstance()
            .publish(
                DlqEvent.of(
                    SINK_ERROR,
                    e.getMessage(),
                    OPERATOR_NAME,
                    value != null ? value.toJson() : null,
                    e));
      }
    }

    @Override
    public void flush(boolean endOfInput) {
      // no buffering
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
