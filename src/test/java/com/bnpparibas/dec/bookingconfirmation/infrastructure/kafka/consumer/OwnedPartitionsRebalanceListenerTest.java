package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OwnedPartitionsRebalanceListenerTest {

    private static final String EMEA_TOPIC = "internal.emea";

    @Mock
    private TopicRegionResolver topicRegionResolver;

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();
    private OwnedPartitionsRebalanceListener listener;

    @BeforeEach
    void setUp() {
        listener = new OwnedPartitionsRebalanceListener(ownedPartitions, topicRegionResolver);
    }

    @Test
    void onPartitionsAssigned_recordsOwnership() {
        given(topicRegionResolver.regionFor(EMEA_TOPIC)).willReturn(Region.EMEA);

        listener.onPartitionsAssigned(
                null, List.of(new TopicPartition(EMEA_TOPIC, 2), new TopicPartition(EMEA_TOPIC, 5)));

        assertThat(ownedPartitions.forRegion(Region.EMEA)).containsExactlyInAnyOrder(2, 5);
    }

    @Test
    void onPartitionsRevokedBeforeCommit_releasesOwnership() {
        given(topicRegionResolver.regionFor(EMEA_TOPIC)).willReturn(Region.EMEA);
        listener.onPartitionsAssigned(
                null, List.of(new TopicPartition(EMEA_TOPIC, 2), new TopicPartition(EMEA_TOPIC, 5)));

        listener.onPartitionsRevokedBeforeCommit(null, List.of(new TopicPartition(EMEA_TOPIC, 2)));

        assertThat(ownedPartitions.forRegion(Region.EMEA)).containsExactly(5);
    }

    @Test
    void onPartitionsLost_releasesOwnership() {
        given(topicRegionResolver.regionFor(EMEA_TOPIC)).willReturn(Region.EMEA);
        listener.onPartitionsAssigned(null, List.of(new TopicPartition(EMEA_TOPIC, 2)));

        listener.onPartitionsLost(null, List.of(new TopicPartition(EMEA_TOPIC, 2)));

        assertThat(ownedPartitions.forRegion(Region.EMEA)).isEmpty();
    }
}
