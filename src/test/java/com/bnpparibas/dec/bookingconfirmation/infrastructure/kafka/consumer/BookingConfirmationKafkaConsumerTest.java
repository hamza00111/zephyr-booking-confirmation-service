package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.BatchListenerFailedException;
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
    void onMessages_shouldIngestBatchThenAcknowledge_onSuccess() {
        var records = List.of(record(0L), record(1L));

        consumer.onMessages(records, acknowledgment);

        InOrder inOrder = inOrder(ingestionService, acknowledgment);
        inOrder.verify(ingestionService).ingestBatch(records);
        inOrder.verify(acknowledgment).acknowledge();
    }

    @Test
    void onMessages_shouldNotAcknowledge_whenIngestBatchThrows() {
        var records = List.of(record(0L), record(1L));
        willThrow(new BatchListenerFailedException("Insert failed", 1)).given(ingestionService).ingestBatch(records);

        assertThatThrownBy(() -> consumer.onMessages(records, acknowledgment))
                .isInstanceOf(BatchListenerFailedException.class);

        verify(acknowledgment, never()).acknowledge();
    }

    private static ConsumerRecord<String, String> record(final long offset) {
        return new ConsumerRecord<>("local.booking.trade.internal.amer", 0, offset, "GSS_1", "{}");
    }
}
