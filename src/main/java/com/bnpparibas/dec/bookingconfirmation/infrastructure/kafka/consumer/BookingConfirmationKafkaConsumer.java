package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes all active {@code internal.<region>} topics and writes each poll's records to the inbox
 * in one batch.
 *
 * <p>Manual ack, one commit per poll: the offset is committed only after the whole batch is in the
 * inbox. On a partial failure {@link InboxIngestionService#ingestBatch} throws
 * {@code BatchListenerFailedException(i)} instead of acking — the error handler then commits the
 * successful prefix and retries/parks from the failing record, so nothing is lost and nothing is
 * double-counted (redeliveries dedupe on the idempotency key).
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
    public void onMessages(final List<ConsumerRecord<String, String>> records, final Acknowledgment acknowledgment) {
        inboxIngestionService.ingestBatch(records);
        acknowledgment.acknowledge();
    }
}
