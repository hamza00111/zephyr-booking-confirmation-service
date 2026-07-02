package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import org.junit.jupiter.api.Test;

class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    void consumerFactory_isBuilt_withPlaintextWhenNoSsl() {
        var factory = config.consumerFactory(
                "localhost:9092", "booking-confirmation-consumer-group", "earliest", 500, "", "", "", "", "", "");

        assertThat(factory).isNotNull();
    }

    @Test
    void kafkaListenerContainerFactory_isBuilt() {
        var consumerFactory = config.consumerFactory(
                "localhost:9092", "booking-confirmation-consumer-group", "earliest", 500, "", "", "", "", "", "");

        var listenerFactory = config.kafkaListenerContainerFactory(
                consumerFactory, new OwnedPartitions(), mock(TopicRegionResolver.class), 3, 2000L, 3L);

        assertThat(listenerFactory).isNotNull();
    }
}
