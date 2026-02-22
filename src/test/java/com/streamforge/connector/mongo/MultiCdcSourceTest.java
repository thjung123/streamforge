package com.streamforge.connector.mongo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MultiCdcSourceTest {

  @Nested
  class HashModPipelineTest {

    @Test
    void singleSplitReturnsEmptyPipeline() {
      List<Bson> pipeline = MongoChangeStreamSource.buildHashModPipeline(0, 1);
      assertThat(pipeline).isEmpty();
    }

    @Test
    void multipleSplitsReturnsMatchStage() {
      List<Bson> pipeline = MongoChangeStreamSource.buildHashModPipeline(2, 4);
      assertThat(pipeline).hasSize(1);

      Document match = (Document) pipeline.get(0);
      assertThat(match.containsKey("$match")).isTrue();

      // verify the nested structure: $match.$expr.$eq[1] == splitIndex
      Document expr = match.get("$match", Document.class).get("$expr", Document.class);
      @SuppressWarnings("unchecked")
      List<Object> eq = (List<Object>) expr.get("$eq");
      assertThat(eq.get(1)).isEqualTo(2);

      // verify $mod contains numSplits
      @SuppressWarnings("unchecked")
      List<Object> mod = ((Document) eq.get(0)).get("$mod", List.class);
      assertThat(mod.get(1)).isEqualTo(4);
    }

    @Test
    void differentSplitIndicesProduceDifferentFilters() {
      List<Bson> pipeline0 = MongoChangeStreamSource.buildHashModPipeline(0, 3);
      List<Bson> pipeline1 = MongoChangeStreamSource.buildHashModPipeline(1, 3);
      List<Bson> pipeline2 = MongoChangeStreamSource.buildHashModPipeline(2, 3);

      // each pipeline should have a different splitIndex in $eq
      assertThat(extractSplitIndex(pipeline0)).isEqualTo(0);
      assertThat(extractSplitIndex(pipeline1)).isEqualTo(1);
      assertThat(extractSplitIndex(pipeline2)).isEqualTo(2);
    }

    @SuppressWarnings("unchecked")
    private int extractSplitIndex(List<Bson> pipeline) {
      Document match = (Document) pipeline.get(0);
      Document expr = match.get("$match", Document.class).get("$expr", Document.class);
      List<Object> eq = (List<Object>) expr.get("$eq");
      return (int) eq.get(1);
    }
  }

  @Nested
  class MongoChangeStreamFlinkTest {

    @Test
    void defaultConstructorUsesNoFilter() {
      MongoChangeStreamSource.MongoChangeStreamFlink flink =
          new MongoChangeStreamSource.MongoChangeStreamFlink();
      // should not throw — verifies construction with default (0, 1)
      assertThat(flink.getBoundedness())
          .isEqualTo(org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void parameterizedConstructorAcceptsSplitConfig() {
      MongoChangeStreamSource.MongoChangeStreamFlink flink =
          new MongoChangeStreamSource.MongoChangeStreamFlink(3, 8);
      assertThat(flink.getBoundedness())
          .isEqualTo(org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED);
    }
  }
}
