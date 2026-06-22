package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
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

    static final String INBOUND_IDEMPOTENCY_HEADER = "cdc-idempotency-key";

    private final InboxRepository inboxRepository;
    private final TopicRegionResolver topicRegionResolver;
    private final TradeEventCodec tradeEventCodec;

    public InboxIngestionService(
            final InboxRepository inboxRepository,
            final TopicRegionResolver topicRegionResolver,
            final TradeEventCodec tradeEventCodec) {
        this.inboxRepository = inboxRepository;
        this.topicRegionResolver = topicRegionResolver;
        this.tradeEventCodec = tradeEventCodec;
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
                log.debug("[{}] Ingested message idempotencyKey={}", region, idempotencyKey);
            } else {
                log.debug("[{}] Duplicate message idempotencyKey={} — already in inbox", region, idempotencyKey);
            }
        }
    }

    private String idempotencyKey(final ConsumerRecord<String, String> record) {
        final Header header = record.headers().lastHeader(INBOUND_IDEMPOTENCY_HEADER);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }
}
