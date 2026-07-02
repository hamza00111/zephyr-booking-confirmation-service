package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class InboxMessageTest {

    @Test
    void accessors_returnConstructorValues() {
        var message = new InboxMessage(
                1L, Region.EMEA, "idem", "internal.emea", 2, 10L, "key", "raw", "trace", "headers");

        assertThat(message.id()).isEqualTo(1L);
        assertThat(message.region()).isEqualTo(Region.EMEA);
        assertThat(message.idempotencyKey()).isEqualTo("idem");
        assertThat(message.sourceTopic()).isEqualTo("internal.emea");
        assertThat(message.partition()).isEqualTo(2);
        assertThat(message.offset()).isEqualTo(10L);
        assertThat(message.messageKey()).isEqualTo("key");
        assertThat(message.rawPayload()).isEqualTo("raw");
        assertThat(message.traceId()).isEqualTo("trace");
        assertThat(message.headers()).isEqualTo("headers");
    }
}
