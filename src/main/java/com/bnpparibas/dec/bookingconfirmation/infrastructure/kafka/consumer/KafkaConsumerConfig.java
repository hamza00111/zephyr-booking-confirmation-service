package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.KafkaSslSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer factory + listener container factory for inbox ingestion.
 *
 * <p>When the error handler's retry budget is exhausted (or the failure is classified
 * non-retryable), the record is <em>parked</em> in the inbox as {@code INGEST_FAILURE} via
 * {@link InboxIngestionService#park} — never silently dropped. If parking itself fails, the error
 * handler seeks back and Kafka redelivers. Invariant: {@code retryBackoffMs * retryMaxAttempts}
 * plus the park round-trip must stay well below {@code max.poll.interval.ms}.
 *
 * <p>Manual immediate ack: the offset is committed only after the inbox write succeeds, so a crash
 * mid-ingestion causes Kafka redelivery (no message loss). On a transient failure (e.g. DB blip) the
 * error handler retries with backoff before the offset advances. For strict no-loss on a persistent
 * failure, harden later by routing to a dead-letter topic or extending the retry budget.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") final String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") final String groupId,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") final String autoOffsetReset,
            @Value("${app.booking-confirmation.consumer.max-poll-records:500}") final int maxPollRecords,
            @Value("${app.booking-confirmation.consumer.max-poll-interval-ms:300000}") final int maxPollIntervalMs,
            @Value("${kafka.ssl.key-store-location:}") final String keyStoreLocation,
            @Value("${kafka.ssl.key-store-type:}") final String keyStoreType,
            @Value("${kafka.ssl.key-store-password:}") final String keyStorePassword,
            @Value("${kafka.ssl.trust-store-location:}") final String trustStoreLocation,
            @Value("${kafka.ssl.trust-store-type:}") final String trustStoreType,
            @Value("${kafka.ssl.trust-store-password:}") final String trustStorePassword) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.putAll(KafkaSslSupport.sslProperties(
                keyStoreLocation,
                keyStoreType,
                keyStorePassword,
                trustStoreLocation,
                trustStoreType,
                trustStorePassword));
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            final ConsumerFactory<String, String> consumerFactory,
            final OwnedPartitions ownedPartitions,
            final TopicRegionResolver topicRegionResolver,
            final InboxIngestionService inboxIngestionService,
            @Value("${app.booking-confirmation.consumer.concurrency:3}") final int concurrency,
            @Value("${app.booking-confirmation.consumer.retry-backoff-ms:2000}") final long retryBackoffMs,
            @Value("${app.booking-confirmation.consumer.retry-max-attempts:3}") final long retryMaxAttempts) {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        // Track which partitions this instance owns so the scheduled drains can scope by them (ADR 0001).
        factory.getContainerProperties()
                .setConsumerRebalanceListener(
                        new OwnedPartitionsRebalanceListener(ownedPartitions, topicRegionResolver));

        factory.setCommonErrorHandler(errorHandler(inboxIngestionService, retryBackoffMs, retryMaxAttempts));
        return factory;
    }

    static DefaultErrorHandler errorHandler(
            final InboxIngestionService inboxIngestionService,
            final long retryBackoffMs,
            final long retryMaxAttempts) {
        final DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    @SuppressWarnings("unchecked")
                    final ConsumerRecord<String, String> stringRecord = (ConsumerRecord<String, String>) record;
                    inboxIngestionService.park(stringRecord, exception);
                },
                new FixedBackOff(retryBackoffMs, retryMaxAttempts));
        // Deterministic failures (e.g. unmapped topic) will never succeed on retry — park immediately.
        errorHandler.addNotRetryableExceptions(IllegalStateException.class, IllegalArgumentException.class);
        // If parking fails, restart the backoff cycle on redelivery instead of recovering immediately.
        errorHandler.setResetStateOnRecoveryFailure(true);
        return errorHandler;
    }
}
