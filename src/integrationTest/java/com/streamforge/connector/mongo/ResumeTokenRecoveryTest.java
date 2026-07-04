package com.streamforge.connector.mongo;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoCollection;
import com.streamforge.connector.mongo.MongoChangeStreamSource.MongoChangeStreamReader;
import com.streamforge.connector.mongo.util.MongoSplit;
import com.streamforge.core.BaseIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class ResumeTokenRecoveryTest extends BaseIntegrationTest {

  private static final String COLLECTION = "resume_recovery_test";

  @Test
  void restoredReaderReplaysEventsWrittenWhileDown() throws Exception {
    String uri = System.getProperty("MONGO_URI");
    MongoCollection<Document> collection =
        mongoClient.getDatabase(MONGO_DB_NAME).getCollection(COLLECTION);

    MongoChangeStreamReader reader1 = new MongoChangeStreamReader(uri, MONGO_DB_NAME, COLLECTION);
    reader1.start();
    reader1.addSplits(List.of(new MongoSplit(0, 1, null)));
    Thread.sleep(1000);

    collection.insertOne(new Document("_id", "A"));
    List<Document> firstRun = new ArrayList<>();
    pollUntil(reader1, firstRun, idIs("A"), Duration.ofSeconds(20));

    List<MongoSplit> checkpoint = reader1.snapshotState(1L);
    assertThat(checkpoint).hasSize(1);
    assertThat(checkpoint.get(0).resumeToken()).as("resume token must be checkpointed").isNotNull();
    reader1.close();

    collection.insertOne(new Document("_id", "B"));
    collection.insertOne(new Document("_id", "C"));

    MongoChangeStreamReader reader2 = new MongoChangeStreamReader(uri, MONGO_DB_NAME, COLLECTION);
    reader2.start();
    reader2.addSplits(checkpoint);
    List<Document> secondRun = new ArrayList<>();
    pollUntil(reader2, secondRun, idIs("C"), Duration.ofSeconds(20));
    reader2.close();

    List<Object> ids = secondRun.stream().map(ResumeTokenRecoveryTest::idOf).toList();
    assertThat(ids).containsExactly("B", "C");
  }

  private static Predicate<Document> idIs(Object id) {
    return doc -> id.equals(idOf(doc));
  }

  private static Object idOf(Document event) {
    Document full = (Document) event.get("fullDocument");
    return full != null ? full.get("_id") : null;
  }

  private static void pollUntil(
      MongoChangeStreamReader reader,
      List<Document> out,
      Predicate<Document> stop,
      Duration timeout)
      throws Exception {
    ListOutput output = new ListOutput(out);
    long end = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < end) {
      reader.pollNext(output);
      if (out.stream().anyMatch(stop)) {
        return;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("Reader did not emit the expected event within " + timeout);
  }

  private static final class ListOutput implements ReaderOutput<Document> {
    private final List<Document> sink;

    ListOutput(List<Document> sink) {
      this.sink = sink;
    }

    @Override
    public void collect(Document record) {
      sink.add(record);
    }

    @Override
    public void collect(Document record, long timestamp) {
      sink.add(record);
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}

    @Override
    public SourceOutput<Document> createOutputForSplit(String splitId) {
      return this;
    }

    @Override
    public void releaseOutputForSplit(String splitId) {}
  }
}
