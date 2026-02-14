package com.streamforge.job.sync.cdc.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import java.util.HashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class MongoToKafkaParserTest {

  @Test
  void testInsertEvent() {
    Document doc =
        new Document("op", "insert")
            .append("source", "users")
            .append("fullDocument", Map.of("_id", 1, "name", "Alice"));

    StreamEnvelop result = MongoToKafkaParser.from(doc);

    assertThat(result.getOperation()).isEqualTo("insert");
    assertThat(result.getPayloadAsMap()).containsEntry("_id", 1);
    assertThat(result.getPrimaryKey()).isEqualTo("1");
  }

  @Test
  void testUpdateEvent_withUpdatedFieldsAndDocumentKey() {
    Document doc =
        new Document("op", "update")
            .append("source", "users")
            .append(
                "updateDescription",
                new Document("updatedFields", new HashMap<>(Map.of("age", 30))))
            .append("documentKey", new Document("_id", 5));

    StreamEnvelop result = MongoToKafkaParser.from(doc);

    assertThat(result.getOperation()).isEqualTo("update");
    assertThat(result.getPayloadAsMap()).containsEntry("_id", 5);
    assertThat(result.getPayloadAsMap()).containsEntry("age", 30);
    assertThat(result.getPrimaryKey()).isEqualTo("5");
  }

  @Test
  void testDeleteEvent_withDocumentKey() {
    Document doc =
        new Document("op", "delete")
            .append("source", "users")
            .append("documentKey", new Document("_id", 7));

    StreamEnvelop result = MongoToKafkaParser.from(doc);

    assertThat(result.getOperation()).isEqualTo("delete");
    assertThat(result.getPayloadAsMap()).containsEntry("_id", 7);
    assertThat(result.getPrimaryKey()).isEqualTo("7");
  }
}
