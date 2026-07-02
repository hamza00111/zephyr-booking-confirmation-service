package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaProducerConfigTest {

    private final KafkaProducerConfig config = new KafkaProducerConfig();

    @Test
    void producerFactory_isBuilt_withPlaintextWhenNoSsl() {
        var factory = config.producerFactory(
                "localhost:9092", "lz4", 20, 65536, 15000, 10000, 5000, "", "", "", "", "", "");

        assertThat(factory).isNotNull();
    }

    @Test
    void kafkaTemplate_wrapsTheProducerFactory() {
        var factory = config.producerFactory(
                "localhost:9092", "lz4", 20, 65536, 15000, 10000, 5000, "", "", "", "", "", "");

        assertThat(config.kafkaTemplate(factory)).isNotNull();
    }
}
