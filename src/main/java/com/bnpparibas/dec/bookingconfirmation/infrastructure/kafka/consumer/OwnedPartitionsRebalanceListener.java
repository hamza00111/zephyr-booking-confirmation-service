package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * Bridges Kafka partition ownership to the scheduled drains. As this instance's consumers gain or
 * lose partitions of the {@code internal.<region>} topics, it updates {@link OwnedPartitions} so
 * PROCESS/RELAY/REQUEUE only claim rows for partitions this instance owns (ADR 0001).
 *
 * <p>Removing on revoke/loss is the fence: once a partition is released, the next drain tick stops
 * selecting it, so its keys are handed cleanly to the new owner.
 */
public class OwnedPartitionsRebalanceListener implements ConsumerAwareRebalanceListener {

    private final OwnedPartitions ownedPartitions;
    private final TopicRegionResolver topicRegionResolver;

    public OwnedPartitionsRebalanceListener(
            final OwnedPartitions ownedPartitions, final TopicRegionResolver topicRegionResolver) {
        this.ownedPartitions = ownedPartitions;
        this.topicRegionResolver = topicRegionResolver;
    }

    @Override
    public void onPartitionsAssigned(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            ownedPartitions.add(topicRegionResolver.regionFor(partition.topic()), partition.partition());
        }
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(
            final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
        release(partitions);
    }

    @Override
    public void onPartitionsLost(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
        release(partitions);
    }

    private void release(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            ownedPartitions.remove(topicRegionResolver.regionFor(partition.topic()), partition.partition());
        }
    }
}
