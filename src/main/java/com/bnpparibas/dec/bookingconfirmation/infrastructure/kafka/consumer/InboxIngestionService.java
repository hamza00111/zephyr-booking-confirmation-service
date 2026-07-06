package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.KafkaHeaderCodec;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

/**
 * Writes a consumed record into the inbox (CONSUME stage).
 *
 * <p>Dedupe key: the inbound {@value #INBOUND_IDEMPOTENCY_HEADER} header if present (set by the
 * upstream publisher), else {@code topic-partition-offset}. The unique constraint on
 * {@code (REGION, IDEMPOTENCY_KEY)} makes redelivery a no-op so the listener can safely ack.
 */
@Service
@Slf4j
public class InboxIngestionService {

    static final String INBOUND_IDEMPOTENCY_HEADER = "idempotency-key";

    private final InboxRepository inboxRepository;
    private final TopicRegionResolver topicRegionResolver;
    private final TradeEventCodec tradeEventCodec;
    private final BookingConfirmationMetrics metrics;

    public InboxIngestionService(
            final InboxRepository inboxRepository,
            final TopicRegionResolver topicRegionResolver,
            final TradeEventCodec tradeEventCodec,
            final BookingConfirmationMetrics metrics) {
        this.inboxRepository = inboxRepository;
        this.topicRegionResolver = topicRegionResolver;
        this.tradeEventCodec = tradeEventCodec;
        this.metrics = metrics;
    }

    public void ingest(final ConsumerRecord<String, String> record) {
        final Region region = topicRegionResolver.regionFor(record.topic());

        if (record.value() == null) {
            log.warn("[{}] Null payload at {}-{}@{} — skipping", region, record.topic(), record.partition(), record.offset());
            return;
        }

        final String traceId = tradeEventCodec.traceId(record.value()).orElse(null);
        final String headers = KafkaHeaderCodec.toJson(record.headers());
        try (var ignored = TraceMdc.scope(traceId)) {
            final String idempotencyKey = idempotencyKey(record);
            final InboxMessage message = new InboxMessage(
                    null,
                    region,
                    idempotencyKey,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value(),
                    traceId,
                    headers);

            final boolean inserted = inboxRepository.insertIfAbsent(message);
            if (inserted) {
                metrics.inboxIngested(region);
                log.debug("[{}] Ingested message idempotencyKey={}", region, idempotencyKey);
            } else {
                metrics.inboxDuplicate(region);
                log.debug("[{}] Duplicate message idempotencyKey={} — already in inbox", region, idempotencyKey);
            }
        }
    }

    /**
     * Parks a record the consumer error handler gave up on (retries exhausted or the failure was
     * classified non-retryable), so the offset can advance without losing the message. The row lands
     * as {@code INGEST_FAILURE} with the raw payload preserved; the REQUEUE stage later promotes it
     * back to {@code NEW} under the inbox retry budget.
     *
     * <p>If the park insert itself fails (e.g. DB down), the exception propagates so the error
     * handler seeks back and Kafka redelivers — parking never trades an ingestion failure for loss.
     * Over-length metadata fields are clamped to their column limits so a data-shaped failure (e.g.
     * an oversized trace id) cannot make the park insert fail the same way ingestion did.
     */
    public void park(final ConsumerRecord<String, String> record, final Exception failure) {
        if (record.value() == null) {
            log.warn(
                    "Null payload at {}-{}@{} reached the recoverer — skipping (tombstones are not ingested)",
                    record.topic(),
                    record.partition(),
                    record.offset());
            return;
        }
        // REGION is NOT NULL, so an unroutable topic cannot be parked: regionFor throws here too and
        // the record redelivers in a loop (loud, no loss). Unreachable while the listener subscribes
        // only to @topicRegionResolver.internalTopics() — the same map regionFor reads.
        final Region region = topicRegionResolver.regionFor(record.topic());
        final Throwable rootFailure = failure.getCause() != null ? failure.getCause() : failure;
        final String traceId =
                clamp(tradeEventCodec.traceId(record.value()).orElse(null), 64);
        try (var ignored = TraceMdc.scope(traceId)) {
            final InboxMessage message = new InboxMessage(
                    null,
                    region,
                    clamp(idempotencyKey(record), 512),
                    clamp(record.topic(), 255),
                    record.partition(),
                    record.offset(),
                    clamp(record.key(), 512),
                    record.value(),
                    traceId,
                    KafkaHeaderCodec.toJson(record.headers()));

            final boolean inserted = inboxRepository.insertParked(message, rootFailure.toString());
            if (inserted) {
                metrics.inboxParked(region);
                log.error(
                        "[{}] Parked record {}-{}@{} as INGEST_FAILURE after ingestion gave up",
                        region,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        rootFailure);
            } else {
                log.warn(
                        "[{}] Record {}-{}@{} already in inbox — nothing to park",
                        region,
                        record.topic(),
                        record.partition(),
                        record.offset());
            }
        }
    }

    private static String clamp(final String value, final int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private String idempotencyKey(final ConsumerRecord<String, String> record) {
        final Header header = record.headers().lastHeader(INBOUND_IDEMPOTENCY_HEADER);
        if (header != null && header.value() != null) {
            final String value = new String(header.value(), StandardCharsets.UTF_8);
            if (!value.isBlank()) {
                return value;
            }
            // A blank header would collapse every such message into one dedupe key, silently
            // dropping all but the first — treat it as absent and surface the upstream defect.
            log.warn(
                    "Blank {} header at {}-{}@{} — falling back to coordinates",
                    INBOUND_IDEMPOTENCY_HEADER,
                    record.topic(),
                    record.partition(),
                    record.offset());
        }
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }
}
