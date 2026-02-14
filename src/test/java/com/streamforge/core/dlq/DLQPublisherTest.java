package com.streamforge.core.dlq;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.streamforge.core.model.DlqEvent;
import java.lang.reflect.Field;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"unchecked"})
class DLQPublisherTest {

  private KafkaProducer<String, String> mockProducer;

  @BeforeEach
  void setUp() throws Exception {
    resetInstance();

    mockProducer = mock(KafkaProducer.class);

    // Create DLQPublisher via mockStatic to avoid real constructor (no real KafkaProducer)
    DLQPublisher publisher = mock(DLQPublisher.class);
    doCallRealMethod().when(publisher).publish(any());

    setField(publisher, "producer", mockProducer);
    setField(publisher, "topic", "test-dlq-topic");
    setStaticField(publisher);
  }

  @AfterEach
  void tearDown() throws Exception {
    resetInstance();
  }

  @Test
  void publish_shouldSendDlqEventSuccessfully() {
    when(mockProducer.send(any(ProducerRecord.class), any())).thenReturn(null);

    DLQPublisher publisher = DLQPublisher.getInstance();
    DlqEvent event = DlqEvent.of("SINK_ERROR", "something failed", "test-sink", "{}", null);

    publisher.publish(event);

    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any());
  }

  @Test
  void publish_shouldHandleExceptionGracefully() {
    when(mockProducer.send(any(ProducerRecord.class), any()))
        .thenThrow(new RuntimeException("send failed"));

    DLQPublisher publisher = DLQPublisher.getInstance();
    DlqEvent event = DlqEvent.of("SINK_ERROR", "broken", "test-sink", "{}", null);

    publisher.publish(event);

    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any());
  }

  private static void resetInstance() throws Exception {
    setStaticField(null);
  }

  private static void setStaticField(Object value) throws Exception {
    Field field = DLQPublisher.class.getDeclaredField("instance");
    field.setAccessible(true);
    field.set(null, value);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = DLQPublisher.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
