package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProcessTypeTest {

    @Test
    void keyFor_combinesRegionAndStage() {
        assertThat(ProcessType.RELAY.keyFor(Region.AMER)).isEqualTo("AMER.RELAY");
        assertThat(ProcessType.PROCESS.keyFor(Region.EMEA)).isEqualTo("EMEA.PROCESS");
        assertThat(ProcessType.REQUEUE.keyFor(Region.APAC)).isEqualTo("APAC.REQUEUE");
    }

    @Test
    void suffix_isLowercaseName() {
        assertThat(ProcessType.PROCESS.suffix()).isEqualTo("process");
        assertThat(ProcessType.RELAY.suffix()).isEqualTo("relay");
        assertThat(ProcessType.REQUEUE.suffix()).isEqualTo("requeue");
    }
}
