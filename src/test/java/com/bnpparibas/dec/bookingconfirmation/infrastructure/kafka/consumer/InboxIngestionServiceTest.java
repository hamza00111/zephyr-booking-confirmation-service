package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InboxIngestionServiceTest {

    private static final String TOPIC = "internal.amer";

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private TopicRegionResolver topicRegionResolver;

    @Mock
    private TradeEventCodec tradeEventCodec;

    @InjectMocks
    private InboxIngestionService service;

    @BeforeEach
    void mapTopicToRegion() {
        given(topicRegionResolver.regionFor(TOPIC)).willReturn(Region.AMER);
    }

    @Test
    void ingest_shouldUseHeaderIdempotencyKeyAndExtractTraceId_whenHeaderPresent() {
        given(tradeEventCodec.traceId("{}")).willReturn(Optional.of("t-1"));
        var record = new ConsumerRecord<>(TOPIC, 0, 7L, "GSS_1", "{}");
        record.headers().add("cdc-idempotency-key", "idem-9".getBytes(UTF_8));

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
    void ingest_shouldSkip_whenPayloadIsNull() {
        var record = new ConsumerRecord<String, String>(TOPIC, 0, 0L, "GSS_1", null);

        service.ingest(record);

        verifyNoInteractions(inboxRepository);
    }
}
