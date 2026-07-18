package com.bnpparibas.dec.bookingconfirmation.common.metric;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MetricsSupportTest {

    private enum TestMetric implements MetricDefinition {
        EVENTS("test.events", "Test events"),
        LATENCY("test.latency", "Test latency"),
        STATUS("test.status", "Events by status");

        private final String key;
        private final String description;

        TestMetric(final String key, final String description) {
            this.key = key;
            this.description = description;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    private enum Status {
        OK,
        FAILED
    }

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MetricsSupport support = new MetricsSupport(registry);

    @Test
    void counter_registersKeyDescriptionAndTags_fromTheMetricDefinition() {
        support.counter(TestMetric.EVENTS).tag("region", "AMER").register().increment(3);

        Counter counter = registry.get("test.events").tag("region", "AMER").counter();
        assertThat(counter.count()).isEqualTo(3.0);
        assertThat(counter.getId().getDescription()).isEqualTo("Test events");
    }

    @Test
    void counter_reRegisteringSameNameAndTags_accumulatesOnOneMeter() {
        support.counter(TestMetric.EVENTS).tag("region", "AMER").register().increment();
        support.counter(TestMetric.EVENTS).tag("region", "AMER").register().increment();
        support.counter(TestMetric.EVENTS).tag("region", "EMEA").register().increment();

        assertThat(registry.get("test.events").tag("region", "AMER").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("test.events").tag("region", "EMEA").counter().count()).isEqualTo(1.0);
    }

    @Test
    void timer_registersAndRecords() {
        support.timer(TestMetric.LATENCY).tag("stage", "PROCESS").register().record(25, TimeUnit.MILLISECONDS);

        assertThat(registry.get("test.latency").tag("stage", "PROCESS").timer().count()).isEqualTo(1);
    }

    @Test
    void gauge_tracksTheSupplier() {
        var value = new java.util.concurrent.atomic.AtomicInteger(7);

        support.gauge(TestMetric.EVENTS, value::get, Tag.of("region", "AMER"));

        assertThat(registry.get("test.events").tag("region", "AMER").gauge().value()).isEqualTo(7.0);
        value.set(11);
        assertThat(registry.get("test.events").tag("region", "AMER").gauge().value()).isEqualTo(11.0);
    }

    @Test
    void statusGauges_registerPerConstant_andRefreshDropsAbsentStatusesToZero() {
        var gauges = support.statusGauges(TestMetric.STATUS, Status.class, "status", Tag.of("source", "KOP"));

        gauges.refresh(Map.of(Status.OK, 5L, Status.FAILED, 2L));
        assertThat(registry.get("test.status").tag("status", "OK").tag("source", "KOP").gauge().value())
                .isEqualTo(5.0);
        assertThat(registry.get("test.status").tag("status", "FAILED").gauge().value()).isEqualTo(2.0);

        gauges.refresh(Map.of(Status.OK, 6L));
        assertThat(registry.get("test.status").tag("status", "OK").tag("source", "KOP").gauge().value())
                .isEqualTo(6.0);
        assertThat(registry.get("test.status").tag("status", "FAILED").gauge().value()).isZero();
    }
}
