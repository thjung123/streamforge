package com.streamforge.job.sync.cdc.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamforge.core.model.StreamEnvelop;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaToMongoParserTest {

  @Test
  void testParse_validJson_shouldReturnStreamEnvelop() throws Exception {
    // given
    StreamEnvelop envelop = StreamEnvelop.of("insert", "orders", Map.of("id", 123));
    String json =
        new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .writeValueAsString(envelop);

    // when
    StreamEnvelop result = KafkaToMongoParser.parseJson(json);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getOperation()).isEqualTo("insert");
    assertThat(result.getSource()).isEqualTo("orders");
    assertThat(result.getPayloadAsMap()).containsEntry("id", 123);
  }

  @Test
  void testParse_invalidJson_shouldReturnNull() {
    // given
    String invalidJson = "{invalid-json}";

    assertThatThrownBy(() -> KafkaToMongoParser.parseJson(invalidJson))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Unexpected character");
  }
}
