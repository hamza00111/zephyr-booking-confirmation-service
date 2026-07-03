package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.resilience.BookingConfirmationCircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes outbox events to {@code published.<region>} with at-least-once delivery and a per-region
 * circuit breaker (mirrors the publisher service's relay publisher).
 *
 * <p>Delivery is at-least-once: the broker ack and the DB {@code markSent} are not atomic, so a crash
 * after ack but before {@code markSent} republishes on the next relay tick. The
 * {@value #HEADER_IDEMPOTENCY_KEY} header (propagated from the inbound message) lets downstream
 * consumers dedupe.
 *
 * <p>Each record independently acquires one breaker permit and reports exactly one outcome, so
 * failure-rate and slow-call thresholds are computed correctly per record.
 */
@Component
@Slf4j
public class KafkaDomainEventPublisher implements DomainEventPublisher {

    public static final String HEADER_IDEMPOTENCY_KEY = "idempotency-key";
    private static final String INBOUND_IDEMPOTENCY_HEADER = "cdc-idempotency-key";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BookingConfirmationCircuitBreakerRegistry breakerRegistry;

    public KafkaDomainEventPublisher(
            final KafkaTemplate<String, String> kafkaTemplate,
            final BookingConfirmationCircuitBreakerRegistry breakerRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.breakerRegistry = breakerRegistry;
    }

    /**
     * Publishes all events for a region. When the breaker is open the whole batch is short-circuited
     * with pre-failed futures so the relay marks them {@code SEND_FAILURE} for the requeue stage.
     *
     * @return map of outbox row id to send future; never null, may be empty.
     */
    @Override
    public Map<Long, CompletableFuture<Void>> sendAll(final Region region, final List<OutboxEvent> events) {
        final Map<Long, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        if (events.isEmpty()) {
            return futures;
        }

        final CircuitBreaker circuitBreaker = breakerRegistry.relayBreaker(region);
        if (!circuitBreaker.tryAcquirePermission()) {
            log.info("[{}] Relay circuit breaker open — short-circuiting {} sends", region, events.size());
            final CallNotPermittedException notPermitted =
                    CallNotPermittedException.createCallNotPermittedException(circuitBreaker);
            events.forEach(event -> futures.put(event.id(), CompletableFuture.failedFuture(notPermitted)));
            return futures;
        }

        events.forEach(event -> {
            try (var ignored = TraceMdc.scope(event.traceId())) {
                final long startNanos = System.nanoTime();
                try {
                    final CompletableFuture<?> sendFuture =
                            kafkaTemplate.send(buildRecord(event)).toCompletableFuture();
                    sendFuture.whenComplete((result, ex) -> {
                        final long elapsedNanos = System.nanoTime() - startNanos;
                        if (ex != null) {
                            circuitBreaker.onError(elapsedNanos, TimeUnit.NANOSECONDS, ex);
                        } else {
                            circuitBreaker.onSuccess(elapsedNanos, TimeUnit.NANOSECONDS);
                        }
                    });
                    // The result value is irrelevant to the relay — expose completion/failure only.
                    futures.put(event.id(), sendFuture.thenAccept(ignored -> {}));
                } catch (final RuntimeException synchronousFailure) {
                    final long elapsedNanos = System.nanoTime() - startNanos;
                    circuitBreaker.onError(elapsedNanos, TimeUnit.NANOSECONDS, synchronousFailure);
                    log.error("[{}] Synchronous send failure for outbox id={}", region, event.id(), synchronousFailure);
                    futures.put(event.id(), CompletableFuture.failedFuture(synchronousFailure));
                }
            }
        });
        return futures;
    }

    private ProducerRecord<String, String> buildRecord(final OutboxEvent event) {
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(event.destination(), null, event.messageKey(), event.payload());
        // Re-emit the persisted inbound headers (minus the CDC-internal idempotency key, re-stamped below).
        KafkaHeaderCodec.fromJson(event.headers()).forEach((key, value) -> {
            if (!INBOUND_IDEMPOTENCY_HEADER.equals(key)) {
                record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        record.headers()
                .add(HEADER_IDEMPOTENCY_KEY, event.idempotencyKey().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
