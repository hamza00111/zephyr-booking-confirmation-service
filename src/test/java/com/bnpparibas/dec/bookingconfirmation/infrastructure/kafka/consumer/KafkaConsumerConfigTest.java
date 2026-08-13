package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Wiring smoke tests plus the error-handler contract that protects against message loss: exhausted
 * or non-retryable failures are parked in the inbox (never silently skipped), and deterministic
 * failures skip the pointless backoff retries.
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigTest {

    @Mock
    private InboxIngestionService inboxIngestionService;

    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private final MessageListenerContainer container =
            mock(MessageListenerContainer.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));

    KafkaConsumerConfigTest() {
        // The commit-recovered gate reads the container's ack mode — mirror the production config.
        var containerProperties = new ContainerProperties("internal.amer");
        containerProperties.setAckMode(AckMode.MANUAL_IMMEDIATE);
        org.mockito.Mockito.doReturn(containerProperties).when(container).getContainerProperties();
    }

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    void consumerFactory_isBuilt_withPlaintextWhenNoSsl() {
        var factory = config.consumerFactory(
                "localhost:9092", "booking-confirmation-consumer-group", "earliest", 500, 300000,
                "", "", "", "", "", "");

        assertThat(factory).isNotNull();
    }

    @Test
    void kafkaListenerContainerFactory_isBuilt() {
        var consumerFactory = config.consumerFactory(
                "localhost:9092", "booking-confirmation-consumer-group", "earliest", 500, 300000,
                "", "", "", "", "", "");

        var listenerFactory = config.kafkaListenerContainerFactory(
                consumerFactory,
                new OwnedPartitions(),
                mock(TopicRegionResolver.class),
                inboxIngestionService,
                3,
                2000L,
                3L);

        assertThat(listenerFactory).isNotNull();
        // Batch mode is the throughput contract: one poll, one listener call, one commit.
        assertThat(listenerFactory.isBatchListener()).isTrue();
    }

    @Test
    void errorHandler_shouldParkImmediately_whenExceptionNotRetryable() {
        final DefaultErrorHandler handler = KafkaConsumerConfig.errorHandler(inboxIngestionService, 0, 3);
        final var record = new ConsumerRecord<>("internal.amer", 0, 7L, "K1", "{}");
        final var failure =
                new ListenerExecutionFailedException("listener failed", new IllegalStateException("unmapped topic"));

        handleUntilRecovered(handler, failure, record, 1);

        verify(inboxIngestionService).park(eq(record), any());
    }

    @Test
    void errorHandler_shouldParkAfterRetries_whenExceptionRetryable() {
        final DefaultErrorHandler handler = KafkaConsumerConfig.errorHandler(inboxIngestionService, 0, 2);
        final var record = new ConsumerRecord<>("internal.amer", 0, 7L, "K1", "{}");
        final var failure = new ListenerExecutionFailedException("listener failed", new RuntimeException("db blip"));

        // First attempts re-seek for redelivery (throw), the final one recovers by parking.
        handleUntilRecovered(handler, failure, record, 5);

        verify(inboxIngestionService).park(eq(record), any());
    }

    @Test
    void errorHandler_shouldCommitRecoveredOffset_afterSuccessfulPark() {
        final DefaultErrorHandler handler = KafkaConsumerConfig.errorHandler(inboxIngestionService, 0, 3);
        final var record = new ConsumerRecord<>("internal.amer", 0, 7L, "K1", "{}");
        final var failure =
                new ListenerExecutionFailedException("listener failed", new IllegalStateException("unmapped topic"));

        handleUntilRecovered(handler, failure, record, 1);

        // MANUAL_IMMEDIATE never acked the parked record — the handler must commit its offset, or
        // the committed position stays behind it and every restart redelivers and re-parks it.
        verify(consumer).commitSync(anyMap(), any());
    }

    @Test
    void errorHandler_shouldNotCommitOffset_whenParkingFails() {
        final DefaultErrorHandler handler = KafkaConsumerConfig.errorHandler(inboxIngestionService, 0, 0);
        final var record = new ConsumerRecord<>("internal.amer", 0, 7L, "K1", "{}");
        final var failure = new ListenerExecutionFailedException("listener failed", new RuntimeException("db blip"));
        willThrow(new RuntimeException("park insert failed — db down"))
                .given(inboxIngestionService)
                .park(any(), any());

        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                handler.handleRemaining(failure, List.of(record), consumer, container);
            } catch (final KafkaException expectedReseek) {
                // recovery failed → seek back for redelivery
            }
        }

        // No successful park, no commit: Kafka must redeliver — parking never trades loss for progress.
        verify(consumer, never()).commitSync(anyMap(), any());
    }

    @Test
    void errorHandler_shouldNotPark_whileRetryBudgetRemains() {
        final DefaultErrorHandler handler = KafkaConsumerConfig.errorHandler(inboxIngestionService, 0, 2);
        final var record = new ConsumerRecord<>("internal.amer", 0, 7L, "K1", "{}");
        final var failure = new ListenerExecutionFailedException("listener failed", new RuntimeException("db blip"));

        try {
            handler.handleRemaining(failure, List.of(record), consumer, container);
        } catch (final KafkaException expectedReseek) {
            // first failure → seek back for redelivery
        }

        verify(inboxIngestionService, never()).park(any(), any());
    }

    private void handleUntilRecovered(
            final DefaultErrorHandler handler,
            final ListenerExecutionFailedException failure,
            final ConsumerRecord<String, String> record,
            final int maxAttempts) {
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                handler.handleRemaining(failure, List.of(record), consumer, container);
                return; // recovered (parked) without re-throwing
            } catch (final KafkaException reseek) {
                // not recovered yet — the container would redeliver; loop simulates that
            }
        }
        throw new AssertionError("Error handler never recovered the record within " + maxAttempts + " attempts");
    }
}
