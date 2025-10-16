package com.flinkcdc.common.dlq;

import com.flinkcdc.common.model.DlqEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.lang.reflect.Field;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DLQPublisherTest {

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("DLQ_TOPIC", "test-dlq-topic");
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        var field = DLQPublisher.class.getDeclaredField("instance");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    void publish_shouldSendDlqEventSuccessfully() throws Exception {
        Field instanceField = DLQPublisher.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);

        Future<RecordMetadata> mockFuture = mock(Future.class);
        try (MockedConstruction<KafkaProducer> mocked = mockConstruction(KafkaProducer.class, (mockProducer, context) -> {
            when(mockProducer.send(any(ProducerRecord.class), any())).thenReturn(mockFuture);
        })) {
            DLQPublisher publisher = DLQPublisher.getInstance();
            DlqEvent event = DlqEvent.of("SINK_ERROR", "something failed", "test-sink", "{}", null);

            // when
            publisher.publish(event);

            // then
            KafkaProducer<String, String> mockProducer = mocked.constructed().get(0);
            verify(mockProducer, times(1)).send(any(ProducerRecord.class), any());
            verify(mockFuture, atMostOnce()).get();
        }
    }
    @Test
    void publish_shouldHandleExceptionGracefully() throws Exception {
        // given
        try (MockedConstruction<KafkaProducer> mocked = mockConstruction(KafkaProducer.class, (mockProducer, context) -> {
            when(mockProducer.send(any())).thenThrow(new RuntimeException("send failed"));
        })) {
            DLQPublisher publisher = DLQPublisher.getInstance();
            DlqEvent event = DlqEvent.of("SINK_ERROR", "broken", "test-sink", "{}", null);

            // when / then
            publisher.publish(event);
        }
    }
}
