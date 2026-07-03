package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

class KafkaSslSupportTest {

    @Test
    void sslProperties_areEmpty_whenNoKeystoreLocation() {
        assertThat(KafkaSslSupport.sslProperties("", "", "", "", "", "")).isEmpty();
    }

    @Test
    void sslProperties_buildFullSslMap_whenConfigured() {
        var props = KafkaSslSupport.sslProperties("ks.jks", "JKS", "ksPw", "ts.jks", "JKS", "tsPw");

        assertThat(props)
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ks.jks")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "ts.jks");
    }
}
