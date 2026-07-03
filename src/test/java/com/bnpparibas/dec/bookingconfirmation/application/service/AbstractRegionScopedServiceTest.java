package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AbstractRegionScopedServiceTest {

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();

    @Test
    void region_andProcessIdentifier_areExposed() {
        var service = new TestService(Region.EMEA, ownedPartitions, BookingConfirmationMetrics.noop());

        assertThat(service.region()).isEqualTo(Region.EMEA);
        assertThat(service.processIdentifier()).isEqualTo("EMEA.PROCESS");
    }

    @Test
    void tick_skips_whenNoOwnedPartitions() {
        var service = new TestService(Region.EMEA, ownedPartitions, BookingConfirmationMetrics.noop());

        service.tick();

        assertThat(service.ticked).isFalse();
    }

    @Test
    void tick_runsDoTick_withTheOwnedPartitions() {
        ownedPartitions.add(Region.EMEA, 3);
        ownedPartitions.add(Region.EMEA, 7);
        var service = new TestService(Region.EMEA, ownedPartitions, BookingConfirmationMetrics.noop());

        service.tick();

        assertThat(service.ticked).isTrue();
        assertThat(service.received).containsExactlyInAnyOrder(3, 7);
    }

    @Test
    void tick_containsExceptionsFromDoTick_andCountsThem() {
        ownedPartitions.add(Region.EMEA, 3);
        var meterRegistry = new SimpleMeterRegistry();
        var service = new TestService(Region.EMEA, ownedPartitions, new BookingConfirmationMetrics(meterRegistry));
        service.toThrow = new RuntimeException("boom");

        assertThatCode(service::tick).doesNotThrowAnyException();
        assertThat(meterRegistry
                        .counter("bc.tick.errors", "region", "EMEA", "stage", "PROCESS")
                        .count())
                .isEqualTo(1.0);
    }

    /** Minimal concrete stage to exercise the base class. */
    private static final class TestService extends AbstractRegionScopedService {

        private boolean ticked;
        private Set<Integer> received;
        private RuntimeException toThrow;

        private TestService(
                final Region region,
                final OwnedPartitions ownedPartitions,
                final BookingConfirmationMetrics metrics) {
            super(new RegionScope(region, ownedPartitions, metrics));
        }

        @Override
        public ProcessType processType() {
            return ProcessType.PROCESS;
        }

        @Override
        protected void doTick(final Set<Integer> ownedPartitions) {
            this.ticked = true;
            this.received = ownedPartitions;
            if (toThrow != null) {
                throw toThrow;
            }
        }
    }
}
