package com.streamforge.pattern.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MetadataDecoratorTest {

  private Metrics metrics;
  private MetadataDecorator.DecoratorFunction<Map<String, String>> function;

  @BeforeEach
  void setUp() {
    metrics = mock(Metrics.class);
    function =
        new MetadataDecorator.DecoratorFunction<>(
            v -> v, "test-stage", "test", metrics, "TestTask", 0);
  }

  @Test
  @DisplayName("should add stage metadata to event")
  void addMetadata() {
    Map<String, String> event = new HashMap<>();

    function.map(event);

    assertThat(event).containsKey("stage.test-stage.taskName");
    assertThat(event).containsKey("stage.test-stage.subtaskIndex");
    assertThat(event).containsKey("stage.test-stage.processedAt");
    assertThat(event.get("stage.test-stage.taskName")).isEqualTo("TestTask");
    assertThat(event.get("stage.test-stage.subtaskIndex")).isEqualTo("0");
  }

  @Test
  @DisplayName("should count decorated events")
  void countEvents() {
    function.map(new HashMap<>());
    function.map(new HashMap<>());

    verify(metrics, times(2)).inc(MetricKeys.DECORATOR_EVENT_COUNT);
  }

  @Test
  @DisplayName("should pass event through unchanged except for metadata")
  void passThrough() {
    Map<String, String> event = new HashMap<>();
    event.put("existing", "value");

    Map<String, String> result = function.map(event);

    assertThat(result).isSameAs(event);
    assertThat(result).containsEntry("existing", "value");
  }

  @Test
  @DisplayName("should preserve existing metadata from other stages")
  void preserveExisting() {
    Map<String, String> event = new HashMap<>();
    event.put("stage.other-stage.taskName", "OtherTask");

    function.map(event);

    assertThat(event).containsEntry("stage.other-stage.taskName", "OtherTask");
    assertThat(event).containsEntry("stage.test-stage.taskName", "TestTask");
  }
}
