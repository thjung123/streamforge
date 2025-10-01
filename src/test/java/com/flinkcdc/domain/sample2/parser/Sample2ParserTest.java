package com.flinkcdc.domain.sample2.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flinkcdc.common.model.CdcEnvelop;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class Sample2ParserTest {

    @Test
    void testParse_validJson_shouldReturnCdcEnvelop() throws Exception {
        // given
        CdcEnvelop envelop = CdcEnvelop.of("insert", "orders", Map.of("id", 123));
        String json = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .writeValueAsString(envelop);

        // when
        CdcEnvelop result = Sample2Parser.parseJson(json);

        // then
        assertThat(result).isNotNull();
        assertThat(result.getOperation()).isEqualTo("insert");
        assertThat(result.getSource()).isEqualTo("orders");
        assertThat(result.getPayloadAsMap()).containsEntry("id", 123);
    }

    @Test
    void testParse_invalidJson_shouldReturnNull() throws Exception {
        // given
        String invalidJson = "{invalid-json}";

        assertThatThrownBy(() -> Sample2Parser.parseJson(invalidJson))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Unexpected character");

    }
}
