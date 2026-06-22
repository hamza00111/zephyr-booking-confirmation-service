package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

@ExtendWith(MockitoExtension.class)
class BookingConfirmationKafkaConsumerTest {

    @Mock
    private InboxIngestionService ingestionService;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private BookingConfirmationKafkaConsumer consumer;

    @Test
    void onMessage_shouldIngestThenAcknowledge_onSuccess() {
        var record = record();

        consumer.onMessage(record, acknowledgment);

        InOrder inOrder = inOrder(ingestionService, acknowledgment);
        inOrder.verify(ingestionService).ingest(record);
        inOrder.verify(acknowledgment).acknowledge();
    }

    @Test
    void onMessage_shouldNotAcknowledge_whenIngestThrows() {
        var record = record();
        willThrow(new RuntimeException("db down")).given(ingestionService).ingest(record);

        assertThatThrownBy(() -> consumer.onMessage(record, acknowledgment)).isInstanceOf(RuntimeException.class);

        verify(acknowledgment, never()).acknowledge();
    }

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>("local.booking.trade.internal.amer", 0, 0L, "GSS_1", "{}");
    }
}
