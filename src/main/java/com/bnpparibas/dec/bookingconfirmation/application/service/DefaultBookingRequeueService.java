package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;
import java.util.Set;

/**
 * REQUEUE stage: promotes failed rows back to {@code NEW} while their retry count is below the
 * budget, otherwise parks them terminally. Covers both halves of the pipeline, scoped to the owned
 * partitions:
 *
 * <ul>
 *   <li>outbox {@code SEND_FAILURE} → {@code NEW} | {@code RETRY_EXHAUSTED}
 *   <li>inbox {@code PROCESS_FAILURE} / {@code INGEST_FAILURE} → {@code NEW} | {@code INVALID}
 * </ul>
 *
 * <p>Runs on a slower tick than the relay.
 */
public class DefaultBookingRequeueService extends AbstractRegionScopedService implements BookingRequeueService {

    private final int maxRetries;
    private final int processMaxRetries;
    private final OutboxRepository outboxRepository;
    private final InboxRepository inboxRepository;

    public DefaultBookingRequeueService(
            final RegionScope scope,
            final int maxRetries,
            final int processMaxRetries,
            final OutboxRepository outboxRepository,
            final InboxRepository inboxRepository) {
        super(scope);
        this.maxRetries = maxRetries;
        this.processMaxRetries = processMaxRetries;
        this.outboxRepository = outboxRepository;
        this.inboxRepository = inboxRepository;
    }

    @Override
    protected void doTick(final Set<Integer> ownedPartitions) {
        report("outbox", outboxRepository.requeueFailed(region(), ownedPartitions, maxRetries));
        report("inbox", inboxRepository.requeueFailed(region(), ownedPartitions, processMaxRetries));
    }

    private void report(final String stage, final RequeueOutcome outcome) {
        metrics.requeuePromoted(region(), stage, outcome.promoted());
        metrics.requeueExhausted(region(), stage, outcome.exhausted());
        if (outcome.promoted() > 0) {
            log.info("[{}] Requeued {} failed {} row(s)", processIdentifier(), outcome.promoted(), stage);
        }
        if (outcome.exhausted() > 0) {
            // Terminal parking: budget spent. Rows need manual replay/triage — keep this loud.
            log.warn(
                    "[{}] {} {} row(s) exhausted their retry budget and were parked terminally",
                    processIdentifier(),
                    outcome.exhausted(),
                    stage);
        }
    }
}
