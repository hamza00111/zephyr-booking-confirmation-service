package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes all active {@code internal.<region>} topics and writes each record to the inbox.
 *
 * <p>Manual ack: the offset is committed only after a successful inbox write. If {@link
 * InboxIngestionService#ingest} throws (e.g. DB down), the record is not acked and Kafka redelivers —
 * preventing message loss.
 */
@Component
public class BookingConfirmationKafkaConsumer {

    private final InboxIngestionService inboxIngestionService;

    public BookingConfirmationKafkaConsumer(final InboxIngestionService inboxIngestionService) {
        this.inboxIngestionService = inboxIngestionService;
    }

    @KafkaListener(
            topics = "#{@topicRegionResolver.internalTopics()}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(final ConsumerRecord<String, String> record, final Acknowledgment acknowledgment) {
        inboxIngestionService.ingest(record);
        acknowledgment.acknowledge();
    }
}
