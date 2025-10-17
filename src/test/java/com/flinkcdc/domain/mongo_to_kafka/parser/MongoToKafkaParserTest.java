package com.flinkcdc.domain.mongo_to_kafka.parser;

import com.flinkcdc.common.model.CdcEnvelop;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MongoToKafkaParserTest {

    @Test
    void testConvertDocumentToCdcEnvelop() {
        // given
        Document doc = new Document("operation", "insert")
                .append("source", "users")
                .append("payload", Map.of("id", 1));

        // when
        CdcEnvelop result = CdcEnvelop.of(
                doc.getString("operation"),
                doc.getString("source"),
                doc.get("payload", Map.class)
        );

        // then
        assertThat(result.getOperation()).isEqualTo("insert");
        assertThat(result.getSource()).isEqualTo("users");
        assertThat(result.getPayloadAsMap()).containsEntry("id", 1);
        assertThat(result.getEventTime()).isNotNull();
        assertThat(result.getProcessedTime()).isNotNull();
    }

    @Test
    void testInvalidDocumentReturnsNullOrThrows() {
        // given
        Document invalidDoc = new Document("operation", null)
                .append("source", null);

        // when
        CdcEnvelop result = null;
        try {
            result = CdcEnvelop.of(
                    invalidDoc.getString("operation"),
                    invalidDoc.getString("source"),
                    invalidDoc.get("payload", Map.class)
            );
        } catch (Exception ignored) {}

        // then
        assertThat(result).isNull();
    }



    @Test
    void testInsertEvent() {
        Document doc = new Document("op", "insert")
                .append("source", "users")
                .append("fullDocument", Map.of("_id", 1, "name", "Alice"));

        CdcEnvelop result = MongoToKafkaParser.from(doc);

        assertThat(result.getOperation()).isEqualTo("insert");
        assertThat(result.getPayloadAsMap()).containsEntry("_id", 1);
        assertThat(result.getPrimaryKey()).isEqualTo("1");
    }

    @Test
    void testUpdateEvent_withUpdatedFieldsAndDocumentKey() {
        Document doc = new Document("op", "update")
                .append("source", "users")
                .append("updateDescription",
                        new Document("updatedFields", new HashMap<>(Map.of("age", 30))))
                .append("documentKey", new Document("_id", 5));

        CdcEnvelop result = MongoToKafkaParser.from(doc);

        assertThat(result.getOperation()).isEqualTo("update");
        assertThat(result.getPayloadAsMap()).containsEntry("_id", 5);
        assertThat(result.getPayloadAsMap()).containsEntry("age", 30);
        assertThat(result.getPrimaryKey()).isEqualTo("5");
    }

    @Test
    void testDeleteEvent_withDocumentKey() {
        Document doc = new Document("op", "delete")
                .append("source", "users")
                .append("documentKey", new Document("_id", 7));

        CdcEnvelop result = MongoToKafkaParser.from(doc);

        assertThat(result.getOperation()).isEqualTo("delete");
        assertThat(result.getPayloadAsMap()).containsEntry("_id", 7);
        assertThat(result.getPrimaryKey()).isEqualTo("7");
    }
}
