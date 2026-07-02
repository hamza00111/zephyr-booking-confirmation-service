package com.bnpparibas.dec.bookingconfirmation.application;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class TraceMdcTest {

    @Test
    void scope_setsTraceId_andClearsOnClose() {
        try (var ignored = TraceMdc.scope("trace-1")) {
            assertThat(MDC.get(TraceMdc.TRACE_ID)).isEqualTo("trace-1");
        }
        assertThat(MDC.get(TraceMdc.TRACE_ID)).isNull();
    }

    @Test
    void scope_mapsNullToEmpty() {
        try (var ignored = TraceMdc.scope(null)) {
            assertThat(MDC.get(TraceMdc.TRACE_ID)).isEmpty();
        }
    }
}
