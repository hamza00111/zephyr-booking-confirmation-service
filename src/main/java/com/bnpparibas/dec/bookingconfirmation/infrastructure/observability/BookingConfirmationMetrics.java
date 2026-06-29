package com.bnpparibas.dec.bookingconfirmation.infrastructure.observability;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.PipelineMetricsRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Registers the no-data-loss observability gauges. Alert on any of these being non-zero / rising:
 *
 * <ul>
 *   <li>{@code booking.confirmation.inbox.rows{status=PARKED|INVALID|PROCESS_FAILURE|BLOCKED}}
 *   <li>{@code booking.confirmation.outbox.rows{status=PARKED|SEND_FAILURE}}
 *   <li>{@code booking.confirmation.outbox.oldest_unsent_seconds} — headline: a message stuck awaiting
 *       delivery (the safety net behind "retry infrastructure failures forever").
 * </ul>
 *
 * <p>Gauges are polled per region on scrape; suppliers run a lightweight indexed count query.
 */
@Component
public class BookingConfirmationMetrics {

    private static final InboxStatus[] INBOX_ALERT_STATUSES = {
        InboxStatus.PARKED, InboxStatus.INVALID, InboxStatus.PROCESS_FAILURE, InboxStatus.BLOCKED
    };
    private static final OutboxStatus[] OUTBOX_ALERT_STATUSES = {OutboxStatus.PARKED, OutboxStatus.SEND_FAILURE};

    private final BookingConfirmationProperties properties;
    private final PipelineMetricsRepository metricsRepository;
    private final MeterRegistry meterRegistry;

    public BookingConfirmationMetrics(
            final BookingConfirmationProperties properties,
            final PipelineMetricsRepository metricsRepository,
            final MeterRegistry meterRegistry) {
        this.properties = properties;
        this.metricsRepository = metricsRepository;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void registerGauges() {
        for (final Region region : properties.activeRegions()) {
            for (final InboxStatus status : INBOX_ALERT_STATUSES) {
                Gauge.builder(
                                "booking.confirmation.inbox.rows",
                                () -> (double) metricsRepository.inboxCount(region, status))
                        .tag("region", region.name())
                        .tag("status", status.name())
                        .description("Inbox rows by status (no-data-loss watch buckets)")
                        .register(meterRegistry);
            }
            for (final OutboxStatus status : OUTBOX_ALERT_STATUSES) {
                Gauge.builder(
                                "booking.confirmation.outbox.rows",
                                () -> (double) metricsRepository.outboxCount(region, status))
                        .tag("region", region.name())
                        .tag("status", status.name())
                        .description("Outbox rows by status (no-data-loss watch buckets)")
                        .register(meterRegistry);
            }
            Gauge.builder(
                            "booking.confirmation.outbox.oldest_unsent_seconds",
                            () -> (double) metricsRepository.oldestUnsentOutboxAgeSeconds(region))
                    .tag("region", region.name())
                    .description("Age of the oldest outbox row still awaiting delivery")
                    .register(meterRegistry);
        }
    }
}
