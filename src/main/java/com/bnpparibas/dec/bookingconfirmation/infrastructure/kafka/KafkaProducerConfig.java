package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Producer factory + {@link KafkaTemplate} for relaying outbox events. String key + String value
 * (the event payload is already serialized JSON). Idempotent producer with {@code acks=all} for
 * at-least-once publishing. Replaces Boot's auto-configured beans (conditional-on-missing-bean).
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") final String bootstrapServers,
            @Value("${spring.kafka.properties.compression.type:lz4}") final String compressionType,
            @Value("${spring.kafka.properties.linger.ms:20}") final int lingerMs,
            @Value("${spring.kafka.properties.batch.size:65536}") final int batchSize,
            @Value("${spring.kafka.properties.delivery.timeout.ms:15000}") final int deliveryTimeoutMs,
            @Value("${spring.kafka.properties.request.timeout.ms:10000}") final int requestTimeoutMs,
            @Value("${spring.kafka.properties.max.block.ms:5000}") final int maxBlockMs,
            @Value("${kafka.ssl.key-store-location:}") final String keyStoreLocation,
            @Value("${kafka.ssl.key-store-type:}") final String keyStoreType,
            @Value("${kafka.ssl.key-store-password:}") final String keyStorePassword,
            @Value("${kafka.ssl.trust-store-location:}") final String trustStoreLocation,
            @Value("${kafka.ssl.trust-store-type:}") final String trustStoreType,
            @Value("${kafka.ssl.trust-store-password:}") final String trustStorePassword) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.putAll(KafkaSslSupport.sslProperties(
                keyStoreLocation,
                keyStoreType,
                keyStorePassword,
                trustStoreLocation,
                trustStoreType,
                trustStorePassword));
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(final ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
