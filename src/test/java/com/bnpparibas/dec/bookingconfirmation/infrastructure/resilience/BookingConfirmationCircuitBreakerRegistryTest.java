package com.bnpparibas.dec.bookingconfirmation.infrastructure.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.ResilienceProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BookingConfirmationCircuitBreakerRegistryTest {

    private final BookingConfirmationCircuitBreakerRegistry registry = new BookingConfirmationCircuitBreakerRegistry(
            new BookingConfirmationProperties(
                    Map.of(), null, null, null, null, null, new ResilienceProperties(50, 50, 3000, 100, 60000, 5, 10)),
            new SimpleMeterRegistry());

    @Test
    void register_thenRelayBreaker_returnsTheBreaker_idempotently() {
        registry.register(Region.AMER);
        var first = registry.relayBreaker(Region.AMER);

        registry.register(Region.AMER); // idempotent — same instance
        assertThat(registry.relayBreaker(Region.AMER)).isSameAs(first);
    }

    @Test
    void relayBreaker_throws_whenRegionNotRegistered() {
        assertThatThrownBy(() -> registry.relayBreaker(Region.EMEA))
                .isInstanceOf(IllegalStateException.class);
    }
}
