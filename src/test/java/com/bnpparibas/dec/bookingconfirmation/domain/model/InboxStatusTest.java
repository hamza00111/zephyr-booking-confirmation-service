package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class InboxStatusTest {

    @Test
    void values_coverTheInboxLifecycle() {
        assertThat(InboxStatus.values())
                .containsExactly(
                        InboxStatus.NEW,
                        InboxStatus.PROCESSED,
                        InboxStatus.AGGREGATED,
                        InboxStatus.PROCESS_FAILURE,
                        InboxStatus.INVALID);
    }

    @Test
    void valueOf_resolvesByName() {
        assertThat(InboxStatus.valueOf("AGGREGATED")).isEqualTo(InboxStatus.AGGREGATED);
    }
}
