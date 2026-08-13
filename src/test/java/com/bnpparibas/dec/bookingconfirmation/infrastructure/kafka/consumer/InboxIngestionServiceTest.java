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
    void ingestBatch_shouldBatchInsertMappedRecords_andSkipTombstones() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        given(inboxRepository.insertAllIfAbsent(any())).willReturn(new int[] {1, 0});
        var tombstone = new ConsumerRecord<String, String>(TOPIC, 0, 1L, "GSS_1", null);
        var records = java.util.List.of(record(0L), tombstone, record(2L));

        service.ingestBatch(records);

        verify(inboxRepository).insertAllIfAbsent(argThat(messages -> messages.size() == 2
                && messages.get(0).offset() == 0L
                && messages.get(1).offset() == 2L));
    }

    @Test
    void ingestBatch_shouldPersistPrefixThenThrowAtFailingIndex_whenMappingFails() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        given(inboxRepository.insertAllIfAbsent(any())).willReturn(new int[] {1, 1});
        given(topicRegionResolver.regionFor("internal.unmapped")).willThrow(new IllegalStateException("unmapped"));
        var poison = new ConsumerRecord<>("internal.unmapped", 0, 9L, "GSS_1", "{}");
        var records = java.util.List.of(record(0L), record(1L), poison, record(3L));

        assertThatThrownBy(() -> service.ingestBatch(records))
                .isInstanceOf(org.springframework.kafka.listener.BatchListenerFailedException.class)
                .hasMessageContaining("Mapping failed")
                .extracting(e -> ((org.springframework.kafka.listener.BatchListenerFailedException) e).getIndex())
                .isEqualTo(2);

        // The two records before the failing index were persisted BEFORE the throw — the error
        // handler will commit their offsets, so an unpersisted prefix would be silent loss.
        verify(inboxRepository).insertAllIfAbsent(argThat(messages -> messages.size() == 2));
    }

    @Test
    void ingestBatch_shouldIsolatePoisonRow_whenBatchInsertFails() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.empty());
        given(inboxRepository.insertAllIfAbsent(any())).willThrow(new DataAccessResourceFailureException("batch"));
        given(inboxRepository.insertIfAbsent(argThat(m -> m != null && m.offset() == 0L))).willReturn(true);
        given(inboxRepository.insertIfAbsent(argThat(m -> m != null && m.offset() == 2L)))
                .willThrow(new DataAccessResourceFailureException("row"));
        var tombstone = new ConsumerRecord<String, String>(TOPIC, 0, 1L, "GSS_1", null);
        var records = java.util.List.of(record(0L), tombstone, record(2L), record(3L));

        // The tombstone shifts positions: message 1 of the insert list is record index 2.
        assertThatThrownBy(() -> service.ingestBatch(records))
                .isInstanceOf(org.springframework.kafka.listener.BatchListenerFailedException.class)
                .hasMessageContaining("Insert failed")
                .extracting(e -> ((org.springframework.kafka.listener.BatchListenerFailedException) e).getIndex())
                .isEqualTo(2);
    }

    @Test
    void ingestBatch_shouldNotTouchRepository_whenAllRecordsAreTombstones() {
        var records = java.util.List.<ConsumerRecord<String, String>>of(
                new ConsumerRecord<>(TOPIC, 0, 0L, "GSS_1", null),
                new ConsumerRecord<>(TOPIC, 0, 1L, "GSS_2", null));

        service.ingestBatch(records);

        verifyNoInteractions(inboxRepository);
    }

    private static ConsumerRecord<String, String> record(final long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "GSS_1", "{}");
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
