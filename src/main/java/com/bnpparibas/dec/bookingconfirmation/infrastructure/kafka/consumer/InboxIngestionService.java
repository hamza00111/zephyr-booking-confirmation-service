package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.KafkaHeaderCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.BatchListenerFailedException;
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
        if (record.value() == null) {
            log.warn("Null payload at {}-{}@{} — skipping", record.topic(), record.partition(), record.offset());
            return;
        }
        final InboxMessage message = toInboxMessage(record);
        try (var ignored = TraceMdc.scope(message.traceId())) {
            recordOutcome(message, inboxRepository.insertIfAbsent(message));
        }
    }

    /**
     * Batch ingestion (batch listener path): one inbox round trip per consumer poll.
     *
     * <p>Ordering contract with {@code DefaultErrorHandler}: throwing {@link
     * BatchListenerFailedException} with index {@code i} causes the offsets of every record BEFORE
     * {@code i} to be committed. Therefore every record before a throw MUST already be persisted —
     * that is why the mapping-catch flushes the collected prefix before throwing, and why the
     * row-by-row fallback walks strictly in order. Do not "simplify" either without re-reading
     * this comment.
     */
    public void ingestBatch(final List<ConsumerRecord<String, String>> records) {
        final List<InboxMessage> messages = new ArrayList<>(records.size());
        final List<Integer> sourceIndex = new ArrayList<>(records.size());

        for (int i = 0; i < records.size(); i++) {
            final ConsumerRecord<String, String> record = records.get(i);
            if (record.value() == null) {
                // Safe to skip without special handling: the listener's batch-end ack covers it.
                log.warn(
                        "Null payload at {}-{}@{} — skipping (tombstones are not ingested)",
                        record.topic(),
                        record.partition(),
                        record.offset());
                continue;
            }
            try {
                messages.add(toInboxMessage(record));
                sourceIndex.add(i);
            } catch (final Exception mappingFailure) {
                insertAllAndRecordMetrics(messages, sourceIndex); // persist prefix FIRST (see contract)
                throw new BatchListenerFailedException("Mapping failed", mappingFailure, i);
            }
        }

        insertAllAndRecordMetrics(messages, sourceIndex);
    }

    private void insertAllAndRecordMetrics(final List<InboxMessage> messages, final List<Integer> sourceIndex) {
        if (messages.isEmpty()) {
            return;
        }
        try {
            final int[] counts = inboxRepository.insertAllIfAbsent(messages);
            for (int j = 0; j < counts.length; j++) {
                // MERGE reports 1 inserted / 0 duplicate; a driver reporting SUCCESS_NO_INFO (-2)
                // is counted as ingested.
                recordOutcome(messages.get(j), counts[j] != 0);
            }
        } catch (final Exception batchFailure) {
            // Row-by-row replay isolates the poison row. Rows the failed batch DID land are
            // re-inserted here and dedupe to "duplicate" — slight metric skew on this rare path,
            // accepted.
            log.warn("Batch inbox insert of {} records failed — replaying row by row", messages.size(), batchFailure);
            insertPerMessage(messages, sourceIndex);
        }
    }

    private void insertPerMessage(final List<InboxMessage> messages, final List<Integer> sourceIndex) {
        for (int j = 0; j < messages.size(); j++) {
            try {
                recordOutcome(messages.get(j), inboxRepository.insertIfAbsent(messages.get(j)));
            } catch (final Exception rowFailure) {
                throw new BatchListenerFailedException("Insert failed", rowFailure, sourceIndex.get(j));
            }
        }
    }

    private void recordOutcome(final InboxMessage message, final boolean inserted) {
        if (inserted) {
            metrics.inboxIngested(message.region());
            log.debug("[{}] Ingested message idempotencyKey={}", message.region(), message.idempotencyKey());
        } else {
            metrics.inboxDuplicate(message.region());
            log.debug(
                    "[{}] Duplicate message idempotencyKey={} — already in inbox",
                    message.region(),
                    message.idempotencyKey());
        }
    }

    private InboxMessage toInboxMessage(final ConsumerRecord<String, String> record) {
        final Region region = topicRegionResolver.regionFor(record.topic());
        return new InboxMessage(
                null,
                region,
                idempotencyKey(record),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                tradeEventCodec.traceId(record.value()).orElse(null),
                KafkaHeaderCodec.toJson(record.headers()));
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
        final Throwable rootFailure = rootCause(failure);
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

    /**
     * Unwraps to the root cause ({@code ListenerExecutionFailedException} →
     * {@code BatchListenerFailedException} → the real failure) so ERROR_MESSAGE records what
     * actually broke, not the wrapper. Cycle-guarded.
     */
    private static Throwable rootCause(final Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
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
