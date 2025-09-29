package com.flinkcdc.domain.sample1.parser;

import com.flinkcdc.common.model.CdcEnvelop;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class Sample1ParserTest {

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
        assertThat(result.getPayload()).containsEntry("id", 1);
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
}
