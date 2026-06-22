package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 * Builds Kafka SSL client properties from the {@code KAFKA_SSL_*} settings, shared by the producer
 * and consumer factories. When no keystore location is configured the map is empty (plaintext) —
 * convenient for local development.
 */
public final class KafkaSslSupport {

    private KafkaSslSupport() {}

    public static Map<String, Object> sslProperties(
            final String keyStoreLocation,
            final String keyStoreType,
            final String keyStorePassword,
            final String trustStoreLocation,
            final String trustStoreType,
            final String trustStorePassword) {
        final Map<String, Object> props = new HashMap<>();
        if (isBlank(keyStoreLocation)) {
            return props;
        }
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
        if (!isBlank(keyStoreType)) {
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keyStoreType);
        }
        if (!isBlank(trustStoreLocation)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation);
        }
        if (!isBlank(trustStoreType)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, trustStoreType);
        }
        if (!isBlank(trustStorePassword)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
        }
        return props;
    }

    private static boolean isBlank(final String value) {
        return value == null || value.isBlank();
    }
}
