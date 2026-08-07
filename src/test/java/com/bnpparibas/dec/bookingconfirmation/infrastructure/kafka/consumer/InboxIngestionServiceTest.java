package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;

@ExtendWith(MockitoExtension.class)
class InboxIngestionServiceTest {

    private static final String TOPIC = "internal.amer";

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private TopicRegionResolver topicRegionResolver;

    @Mock
    private TradeEventCodec tradeEventCodec;

    private InboxIngestionService service;

    @BeforeEach
    void setUp() {
        service = new InboxIngestionService(
                inboxRepository, topicRegionResolver, tradeEventCodec, BookingConfirmationMetrics.noop());
        // Lenient: the null-payload short-circuits return before resolving the region.
        lenient().when(topicRegionResolver.regionFor(TOPIC)).thenReturn(Region.AMER);
    }

    @Test
    void ingest_shouldUseHeaderIdempotencyKeyAndExtractTraceId_whenHeaderPresent() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.of("t-1"));
        var record = new ConsumerRecord<>(TOPIC, 0, 7L, "GSS_1", "{}");
        record.headers().add("idempotency-key", "idem-9".getBytes(UTF_8));

        service.ingest(record);

        verify(inboxRepository).insertIfAbsent(argThat(m ->
                m.idempotencyKey().equals("idem-9") && m.region() == Region.AMER && "t-1".equals(m.traceId())));
    }

    @Test
    void ingest_shouldFallBackToTopicPartitionOffset_whenNoHeader() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        var record = new ConsumerRecord<>(TOPIC, 0, 5L, "GSS_1", "{}");

        service.ingest(record);

        verify(inboxRepository).insertIfAbsent(argThat(m -> m.idempotencyKey().equals("internal.amer-0-5")));
    }

    @Test
    void ingest_shouldFallBackToTopicPartitionOffset_whenHeaderValueIsEmpty() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        var record = new ConsumerRecord<>(TOPIC, 0, 5L, "GSS_1", "{}");
        record.headers().add("idempotency-key", new byte[0]);

        service.ingest(record);

        verify(inboxRepository).insertIfAbsent(argThat(m -> m.idempotencyKey().equals("internal.amer-0-5")));
    }

    @Test
    void ingest_shouldFallBackToTopicPartitionOffset_whenHeaderValueIsWhitespaceOnly() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        var record = new ConsumerRecord<>(TOPIC, 0, 5L, "GSS_1", "{}");
        record.headers().add("idempotency-key", "   ".getBytes(UTF_8));

        service.ingest(record);

        verify(inboxRepository).insertIfAbsent(argThat(m -> m.idempotencyKey().equals("internal.amer-0-5")));
    }

    @Test
    void ingest_shouldSkip_whenPayloadIsNull() {
        var record = new ConsumerRecord<String, String>(TOPIC, 0, 0L, "GSS_1", null);

        service.ingest(record);

        verifyNoInteractions(inboxRepository);
    }

    @Test
    void park_shouldInsertRowWithRootCauseErrorMessage() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.of("t-1"));
        var record = new ConsumerRecord<>(TOPIC, 0, 7L, "GSS_1", "{}");
        record.headers().add("idempotency-key", "idem-9".getBytes(UTF_8));

        service.park(record, new RuntimeException("listener wrapper", new IllegalStateException("db down")));

        verify(inboxRepository).insertParked(
                argThat(m -> m.idempotencyKey().equals("idem-9")
                        && m.region() == Region.AMER
                        && "t-1".equals(m.traceId())
                        && "{}".equals(m.rawPayload())),
                eq("java.lang.IllegalStateException: db down"));
    }

    @Test
    void park_shouldSkip_whenPayloadIsNull() {
        var record = new ConsumerRecord<String, String>(TOPIC, 0, 0L, "GSS_1", null);

        service.park(record, new RuntimeException("boom"));

        verifyNoInteractions(inboxRepository);
    }

    @Test
    void park_shouldPropagate_whenParkInsertFails() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        given(inboxRepository.insertParked(any(), any())).willThrow(new DataAccessResourceFailureException("db down"));
        var record = new ConsumerRecord<>(TOPIC, 0, 7L, "GSS_1", "{}");

        assertThatThrownBy(() -> service.park(record, new RuntimeException("boom")))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }

    @Test
    void park_shouldClampOversizedMetadataFields() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.of("x".repeat(100)));
        var record = new ConsumerRecord<>(TOPIC, 0, 7L, "k".repeat(600), "{}");
        record.headers().add("idempotency-key", "i".repeat(600).getBytes(UTF_8));

        service.park(record, new RuntimeException("boom"));

        verify(inboxRepository).insertParked(
                argThat(m -> m.idempotencyKey().length() == 512
                        && m.messageKey().length() == 512
                        && m.traceId().length() == 64),
                eq("java.lang.RuntimeException: boom"));
    }
}
