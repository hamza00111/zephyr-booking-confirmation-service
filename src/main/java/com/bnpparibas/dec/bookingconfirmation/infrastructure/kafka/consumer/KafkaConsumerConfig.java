package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.KafkaSslSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecorder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.TransactionException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer factory + listener container factory for inbox ingestion.
 *
 * <p>Manual immediate ack: the offset is committed only after the inbox write succeeds, so a crash
 * mid-ingestion causes Kafka redelivery (no message loss).
 *
 * <p><b>No data loss on persistent failure.</b> The error handler classifies failures:
 * <ul>
 *   <li><b>Infrastructure</b> (DB down, connection/transaction failure) — retried <em>indefinitely</em>
 *       with backoff. The offset never advances, so a DB outage can never silently skip live traffic;
 *       it back-pressures to Kafka and resumes in order once the DB recovers.
 *   <li><b>Poison</b> (any other failure — deserialization, an NPE bug) — retried a bounded number of
 *       times then routed to {@code <topic>.DLT} (durable, alerted, replayable). This never silently
 *       commits-and-skips, and a deterministic bug cannot block a partition forever.
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") final String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") final String groupId,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") final String autoOffsetReset,
            @Value("${app.booking-confirmation.consumer.max-poll-records:500}") final int maxPollRecords,
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
            final KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.booking-confirmation.consumer.concurrency:3}") final int concurrency,
            @Value("${app.booking-confirmation.consumer.retry-backoff-ms:2000}") final long retryBackoffMs,
            @Value("${app.booking-confirmation.consumer.retry-max-attempts:3}") final long retryMaxAttempts,
            @Value("${app.booking-confirmation.consumer.infra-retry-backoff-ms:5000}") final long infraBackoffMs) {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(noLossErrorHandler(kafkaTemplate, retryBackoffMs, retryMaxAttempts, infraBackoffMs));
        return factory;
    }

    /**
     * Poison is recovered to {@code <topic>.DLT} after a bounded retry; infrastructure failures are
     * retried indefinitely so a DB outage neither drops nor dead-letters live traffic.
     */
    private DefaultErrorHandler noLossErrorHandler(
            final KafkaTemplate<String, String> kafkaTemplate,
            final long retryBackoffMs,
            final long retryMaxAttempts,
            final long infraBackoffMs) {
        final DeadLetterPublishingRecorder recoverer = new DeadLetterPublishingRecorder(
                kafkaTemplate, (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        final BackOff poisonBackOff = new FixedBackOff(retryBackoffMs, retryMaxAttempts);
        final BackOff infraBackOff = new FixedBackOff(infraBackoffMs, Long.MAX_VALUE); // effectively infinite

        final DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, poisonBackOff);
        handler.setBackOffFunction((record, exception) ->
                isInfrastructureFailure(exception) ? infraBackOff : poisonBackOff);
        return handler;
    }

    /** True if the failure is a transient infrastructure problem (DB/connection/transaction). */
    private static boolean isInfrastructureFailure(final Exception exception) {
        Throwable cause = exception;
        while (cause != null) {
            // DataAccessException covers Spring's JDBC/connection failures (incl. CannotGetJdbcConnection).
            if (cause instanceof DataAccessException
                    || cause instanceof TransactionException
                    || cause instanceof java.sql.SQLException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
