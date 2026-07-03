package com.bnpparibas.dec.bookingconfirmation.domain.event;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Outbound port for publishing outbox events to the messaging layer.
 *
 * <p>This domain-facing contract is what the application layer (the relay) depends on, so it never
 * references the Kafka adapter directly. The infrastructure {@code KafkaDomainEventPublisher}
 * implements it. Keeping the dependency pointing inward (application -> domain port <- infrastructure
 * adapter) is what lets the messaging technology change without touching the relay logic.
 */
public interface DomainEventPublisher {

    /**
     * Publishes all events for a region, returning a per-event send future keyed by outbox row id.
     * The future carries no value ({@link Void}) — the relay only observes completion or failure.
     *
     * <p>Implementations provide at-least-once delivery and resilience (e.g. a per-region circuit
     * breaker); when delivery is refused the returned futures complete exceptionally so the relay can
     * mark the rows {@code SEND_FAILURE}.
     */
    Map<Long, CompletableFuture<Void>> sendAll(Region region, List<OutboxEvent> events);
}
