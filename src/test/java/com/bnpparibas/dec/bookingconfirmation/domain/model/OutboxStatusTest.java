package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutboxStatusTest {

    @Test
    void values_coverTheOutboxLifecycle() {
        assertThat(OutboxStatus.values())
                .containsExactly(
                        OutboxStatus.NEW,
                        OutboxStatus.SENT,
                        OutboxStatus.SEND_FAILURE,
                        OutboxStatus.RETRY_EXHAUSTED);
    }

    @Test
    void valueOf_resolvesByName() {
        assertThat(OutboxStatus.valueOf("RETRY_EXHAUSTED")).isEqualTo(OutboxStatus.RETRY_EXHAUSTED);
    }
}
