package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.resilience.BookingConfirmationCircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaDomainEventPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private final BookingConfirmationCircuitBreakerRegistry breakerRegistry =
            mock(BookingConfirmationCircuitBreakerRegistry.class);

    private final KafkaDomainEventPublisher publisher =
            new KafkaDomainEventPublisher(kafkaTemplate, breakerRegistry);

    @Test
    void sendAll_returnsEmpty_whenNoEvents() {
        assertThat(publisher.sendAll(Region.AMER, List.of())).isEmpty();
    }

    @Test
    void sendAll_shortCircuitsEverything_whenBreakerOpen() {
        var breaker = CircuitBreaker.ofDefaults("AMER.RELAY");
        breaker.transitionToOpenState();
        when(breakerRegistry.relayBreaker(Region.AMER)).thenReturn(breaker);

        var futures = publisher.sendAll(Region.AMER, List.of(outbox(1L), outbox(2L)));

        assertThat(futures).containsOnlyKeys(1L, 2L);
        assertThat(futures.get(1L)).isCompletedExceptionally();
        assertThat(futures.get(2L)).isCompletedExceptionally();
        // One rejected permit accounted per event, not per batch.
        assertThat(breaker.getMetrics().getNumberOfNotPermittedCalls()).isEqualTo(2);
    }

    @Test
    void sendAll_shouldLetExactlyTheProbeQuotaThrough_whenBreakerHalfOpen() {
        // HALF_OPEN allows exactly permittedNumberOfCallsInHalfOpenState probes; a per-batch permit
        // would let the whole batch through and corrupt the breaker's call accounting.
        var breaker = CircuitBreaker.of(
                "AMER.RELAY",
                CircuitBreakerConfig.custom().permittedNumberOfCallsInHalfOpenState(2).build());
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        when(breakerRegistry.relayBreaker(Region.AMER)).thenReturn(breaker);
        doReturn(new CompletableFuture<Void>()).when(kafkaTemplate).send(any(ProducerRecord.class));

        var events = LongStream.rangeClosed(1, 5).mapToObj(KafkaDomainEventPublisherTest::outbox).toList();
        var futures = publisher.sendAll(Region.AMER, events);

        assertThat(futures).hasSize(5);
        assertThat(futures.values().stream().filter(CompletableFuture::isCompletedExceptionally)).hasSize(3);
    }

    @Test
    void sendAll_sendsEachEvent_whenBreakerClosed() {
        when(breakerRegistry.relayBreaker(Region.AMER)).thenReturn(CircuitBreaker.ofDefaults("AMER.RELAY"));
        doReturn(CompletableFuture.completedFuture(null)).when(kafkaTemplate).send(any(ProducerRecord.class));

        var futures = publisher.sendAll(Region.AMER, List.of(outbox(1L)));

        assertThat(futures).containsOnlyKeys(1L);
        assertThat(futures.get(1L)).isCompleted();
    }

    private static OutboxEvent outbox(long id) {
        return new OutboxEvent(id, Region.AMER, "idem-" + id, "published.amer", "key", 0, "{}", id, "trace", null);
    }
}
