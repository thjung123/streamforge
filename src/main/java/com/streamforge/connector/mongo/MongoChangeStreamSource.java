package com.streamforge.connector.mongo;

import static com.streamforge.connector.mongo.MongoConfigKeys.*;
import static com.streamforge.core.config.ScopedConfig.require;

import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.streamforge.connector.mongo.util.MongoSplit;
import com.streamforge.connector.mongo.util.MongoSplitEnumerator;
import com.streamforge.connector.mongo.util.MongoSplitSerializer;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoChangeStreamSource implements PipelineBuilder.SourceBuilder<Document> {

  @Override
  public DataStream<Document> build(StreamExecutionEnvironment env, String jobName) {
    return build(env, jobName, 0, 1);
  }

  public DataStream<Document> build(
      StreamExecutionEnvironment env, String jobName, int splitIndex, int numSplits) {
    String uri = require(MONGO_URI);
    String db = require(MONGO_DB);
    String collection = require(MONGO_COLLECTION);
    String suffix = numSplits > 1 ? "-MongoSource-" + splitIndex : "-MongoSource";
    return env.fromSource(
        new MongoChangeStreamFlink(splitIndex, numSplits, uri, db, collection),
        WatermarkStrategy.noWatermarks(),
        jobName + suffix);
  }

  static List<Bson> buildHashModPipeline(int splitIndex, int numSplits) {
    if (numSplits <= 1) return Collections.emptyList();
    String json =
        """
        {"$match":{"$expr":{"$eq":[{"$mod":[{"$abs":{"$toHashedIndexKey":"$documentKey._id"}},%d]},%d]}}}
        """
            .formatted(numSplits, splitIndex);
    return Collections.singletonList(Document.parse(json));
  }

  public static class MongoChangeStreamFlink implements Source<Document, MongoSplit, Boolean> {

    private final int splitIndex;
    private final int numSplits;
    private final String uri;
    private final String db;
    private final String collection;

    public MongoChangeStreamFlink() {
      this(0, 1, null, null, null);
    }

    public MongoChangeStreamFlink(int splitIndex, int numSplits) {
      this(splitIndex, numSplits, null, null, null);
    }

    public MongoChangeStreamFlink(
        int splitIndex, int numSplits, String uri, String db, String collection) {
      this.splitIndex = splitIndex;
      this.numSplits = numSplits;
      this.uri = uri;
      this.db = db;
      this.collection = collection;
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Document, MongoSplit> createReader(SourceReaderContext ctx) {
      return new MongoChangeStreamReader(uri, db, collection);
    }

    @Override
    public SplitEnumerator<MongoSplit, Boolean> createEnumerator(
        SplitEnumeratorContext<MongoSplit> ctx) {
      return new MongoSplitEnumerator(ctx, splitIndex, numSplits, false);
    }

    @Override
    public SplitEnumerator<MongoSplit, Boolean> restoreEnumerator(
        SplitEnumeratorContext<MongoSplit> ctx, Boolean checkpoint) {
      return new MongoSplitEnumerator(ctx, splitIndex, numSplits, Boolean.TRUE.equals(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<MongoSplit> getSplitSerializer() {
      return MongoSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
      return new SimpleVersionedSerializer<>() {
        @Override
        public int getVersion() {
          return 1;
        }

        @Override
        public byte[] serialize(Boolean state) {
          return new byte[] {(byte) (Boolean.TRUE.equals(state) ? 1 : 0)};
        }

        @Override
        public Boolean deserialize(int version, byte[] serialized) {
          return serialized.length > 0 && serialized[0] == 1;
        }
      };
    }
  }

  public static class MongoChangeStreamReader implements SourceReader<Document, MongoSplit> {
    private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamReader.class);

    private final String uri;
    private final String db;
    private final String collection;

    private int splitIndex = 0;
    private int numSplits = 1;
    private String resumeToken;

    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
    private volatile boolean running = true;
    private MongoClient client;

    public MongoChangeStreamReader(String uri, String db, String collection) {
      this.uri = uri;
      this.db = db;
      this.collection = collection;
    }

    @Override
    public void start() {}

    private void openCursor() {
      if (client == null) {
        client = MongoClients.create(uri);
      }
      List<Bson> pipeline = buildHashModPipeline(splitIndex, numSplits);
      try {
        cursor = watchCursor(pipeline);
      } catch (MongoException e) {
        if (resumeToken == null) {
          throw e;
        }
        log.warn(
            "[MongoSource] resumeAfter failed for {}.{} ({}); reopening from now with data loss",
            db,
            collection,
            e.getMessage());
        resumeToken = null;
        cursor = watchCursor(pipeline);
      }
      log.info(
          "[MongoSource] Connected to {}.{} (split={}/{}, resumed={})",
          db,
          collection,
          splitIndex,
          numSplits,
          resumeToken != null);
    }

    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> watchCursor(
        List<Bson> pipeline) {
      ChangeStreamIterable<Document> watch =
          client.getDatabase(db).getCollection(collection).watch(pipeline);
      if (resumeToken != null) {
        watch = watch.resumeAfter(BsonDocument.parse(resumeToken));
      }
      return watch.cursor();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Document> output) throws Exception {
      if (!running) {
        return InputStatus.END_OF_INPUT;
      }
      if (cursor == null) {
        Thread.sleep(100);
        return InputStatus.NOTHING_AVAILABLE;
      }

      try {
        if (!cursor.hasNext()) {
          BsonDocument postBatch = cursor.getResumeToken();
          if (postBatch != null) {
            resumeToken = postBatch.toJson();
          }
          Thread.sleep(100);
          return InputStatus.NOTHING_AVAILABLE;
        }

        ChangeStreamDocument<Document> change = cursor.next();
        String dbName =
            change.getNamespace() != null ? change.getNamespace().getDatabaseName() : null;
        String collName =
            change.getNamespace() != null ? change.getNamespace().getCollectionName() : null;

        Document fullDoc = change.getFullDocument();
        Document fullDocument = null;
        if (fullDoc != null) {
          fullDocument = Document.parse(fullDoc.toJson());
        }

        Document event =
            new Document()
                .append(
                    "op",
                    change.getOperationType() != null
                        ? change.getOperationType().getValue()
                        : "unknown")
                .append("db", dbName)
                .append("collection", collName)
                .append("documentKey", change.getDocumentKey())
                .append("fullDocument", fullDocument)
                .append("eventTime", change.getClusterTime());

        output.collect(event);
        BsonDocument token = change.getResumeToken();
        if (token != null) {
          resumeToken = token.toJson();
        }
        return InputStatus.MORE_AVAILABLE;
      } catch (MongoException ex) {
        log.warn("[MongoSource] Lost connection to MongoDB, retrying in 3s...", ex);
        safeCloseCursor();
        Thread.sleep(3000);
        reconnect();
        return InputStatus.NOTHING_AVAILABLE;
      } catch (Exception e) {
        log.error("[MongoSource] Error reading change stream event", e);
        DlqEvent dlqEvent =
            DlqEvent.of("SOURCE_PARSING_ERROR", e.getMessage(), "MongoChangeStreamSource", null, e);
        DLQPublisher.getInstance().publish(dlqEvent);
        throw e;
      }
    }

    @Override
    public List<MongoSplit> snapshotState(long checkpointId) {
      if (cursor == null) {
        return Collections.emptyList();
      }
      return Collections.singletonList(new MongoSplit(splitIndex, numSplits, resumeToken));
    }

    @Override
    public void addSplits(List<MongoSplit> splits) {
      if (splits.isEmpty()) {
        return;
      }
      MongoSplit split = splits.get(0);
      this.splitIndex = split.splitIndex();
      this.numSplits = split.numSplits();
      this.resumeToken = split.resumeToken();
      try {
        openCursor();
      } catch (Exception e) {
        log.error("[MongoSource] Failed to open change stream for {}", split, e);
        throw new RuntimeException("Failed to start Mongo ChangeStream source", e);
      }
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public CompletableFuture<Void> isAvailable() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      running = false;
      try {
        if (cursor != null) cursor.close();
        if (client != null) client.close();
        log.info("[MongoSource] ChangeStream closed gracefully");
      } catch (Exception e) {
        log.warn("[MongoSource] Error while closing resources", e);
      }
    }

    private void reconnect() {
      openCursor();
      log.info(
          "[MongoSource] Reconnected to MongoDB: {}.{} (split={}/{})",
          db,
          collection,
          splitIndex,
          numSplits);
    }

    private void safeCloseCursor() {
      try {
        if (cursor != null) cursor.close();
      } catch (Exception ignored) {
      }
      try {
        if (client != null) client.close();
      } catch (Exception ignored) {
      }
      cursor = null;
      client = null;
    }
  }
}
