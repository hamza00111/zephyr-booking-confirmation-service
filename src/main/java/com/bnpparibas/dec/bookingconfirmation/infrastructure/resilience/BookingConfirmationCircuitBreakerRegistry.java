package com.bnpparibas.dec.bookingconfirmation.infrastructure.resilience;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.ResilienceProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Owns one Resilience4j relay {@link CircuitBreaker} per region (named {@code <REGION>.RELAY}),
 * wrapping Kafka publish calls. Breakers are registered once at startup and live for the application
 * lifetime. Per-region isolation: one region's open breaker never blocks another's sends.
 */
@Component
@Slf4j
public class BookingConfirmationCircuitBreakerRegistry {

    private final ResilienceProperties props;
    private final CircuitBreakerRegistry r4jRegistry = CircuitBreakerRegistry.ofDefaults();
    private final Map<Region, CircuitBreaker> relayBreakers = new ConcurrentHashMap<>();

    public BookingConfirmationCircuitBreakerRegistry(final BookingConfirmationProperties properties) {
        this.props = properties.resilience();
    }

    /** Idempotently registers the relay breaker for a region. */
    public void register(final Region region) {
        relayBreakers.computeIfAbsent(region, r -> {
            final CircuitBreaker breaker = r4jRegistry.circuitBreaker(ProcessType.RELAY.keyFor(r), buildConfig());
            log.info("[{}] Relay circuit breaker registered", ProcessType.RELAY.keyFor(r));
            return breaker;
        });
    }

    public CircuitBreaker relayBreaker(final Region region) {
        final CircuitBreaker breaker = relayBreakers.get(region);
        if (breaker == null) {
            throw new IllegalStateException(
                    "No relay circuit breaker registered for region=" + region + " — register() must run at startup.");
        }
        return breaker;
    }

    private CircuitBreakerConfig buildConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(props.failureRateThreshold())
                .slowCallRateThreshold(props.slowCallRateThreshold())
                .slowCallDurationThreshold(Duration.ofMillis(props.slowCallDurationThresholdMs()))
                .slidingWindowSize(props.slidingWindowSize())
                .waitDurationInOpenState(Duration.ofMillis(props.waitDurationInOpenStateMs()))
                .permittedNumberOfCallsInHalfOpenState(props.permittedCallsInHalfOpenState())
                .minimumNumberOfCalls(props.minimumNumberOfCalls())
                .build();
    }
}
