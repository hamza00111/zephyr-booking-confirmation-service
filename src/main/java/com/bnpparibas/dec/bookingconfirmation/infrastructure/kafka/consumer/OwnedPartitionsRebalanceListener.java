package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
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
 *
 * <p>Ownership transitions are logged: while this instance owns nothing for a region, every drain
 * tick of that region silently skips — without these lines (and the {@code bc.partitions.owned}
 * gauge) a kicked-from-group instance looks identical to a healthy idle one.
 */
@Slf4j
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
        log.info("Partitions assigned: {}", byRegion(partitions));
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(
            final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
        release(partitions);
        log.info("Partitions revoked (rebalance): {}", byRegion(partitions));
    }

    @Override
    public void onPartitionsLost(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
        release(partitions);
        // Lost (not revoked) means the group kicked this instance — e.g. a debugger suspend or a
        // long GC froze the heartbeat past session.timeout.ms. All drains for the affected regions
        // skip until the consumer rejoins and partitions are re-assigned.
        log.warn("Partitions LOST (kicked from group — drains skip until re-assigned): {}", byRegion(partitions));
    }

    private void release(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            ownedPartitions.remove(topicRegionResolver.regionFor(partition.topic()), partition.partition());
        }
    }

    private Map<Region, Collection<Integer>> byRegion(final Collection<TopicPartition> partitions) {
        final Map<Region, Collection<Integer>> grouped = new EnumMap<>(Region.class);
        for (final TopicPartition partition : partitions) {
            grouped.computeIfAbsent(topicRegionResolver.regionFor(partition.topic()), region -> new TreeSet<>())
                    .add(partition.partition());
        }
        return grouped.isEmpty() ? Map.of() : grouped;
    }
}
