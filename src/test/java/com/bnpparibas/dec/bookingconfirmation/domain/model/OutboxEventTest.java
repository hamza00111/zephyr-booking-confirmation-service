package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutboxEventTest {

    @Test
    void accessors_returnConstructorValues() {
        var event = new OutboxEvent(
                1L, Region.AMER, "idem", "published.amer", "key", 2, "payload", 3L, "trace", "headers");

        assertThat(event.id()).isEqualTo(1L);
        assertThat(event.region()).isEqualTo(Region.AMER);
        assertThat(event.idempotencyKey()).isEqualTo("idem");
        assertThat(event.destination()).isEqualTo("published.amer");
        assertThat(event.messageKey()).isEqualTo("key");
        assertThat(event.kafkaPartition()).isEqualTo(2);
        assertThat(event.payload()).isEqualTo("payload");
        assertThat(event.inboxId()).isEqualTo(3L);
        assertThat(event.traceId()).isEqualTo("trace");
        assertThat(event.headers()).isEqualTo("headers");
    }
}
