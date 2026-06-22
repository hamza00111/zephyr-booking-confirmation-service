package com.bnpparibas.dec.bookingconfirmation.application.config;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Root configuration bound from {@code app.booking-confirmation}.
 *
 * <p>Per-region settings live under {@code regions}; per-stage settings (process/relay/requeue) and
 * cross-cutting settings (consumer/scheduler/resilience) are global. Lock TTL for a stage is derived
 * from the stage tick interval and the region's {@code lockTtlMultiplier}.
 */
@ConfigurationProperties(prefix = "app.booking-confirmation")
public record BookingConfirmationProperties(
        Map<Region, RegionProperties> regions,
        ConsumerProperties consumer,
        SchedulerProperties scheduler,
        ProcessProperties process,
        RelayProperties relay,
        RequeueProperties requeue,
        ResilienceProperties resilience) {

    /** Per-region isolation unit: own topics, lock, circuit breaker, scheduled tasks. */
    public record RegionProperties(
            @DefaultValue("true") boolean active,
            @DefaultValue("3") int lockTtlMultiplier,
            String internalTopic,
            String publishedTopic) {}

    /** Kafka consumer (inbox ingestion) tuning. */
    public record ConsumerProperties(
            @DefaultValue("3") int concurrency,
            @DefaultValue("500") int maxPollRecords,
            @DefaultValue("2000") long retryBackoffMs,
            @DefaultValue("3") int retryMaxAttempts) {}

    /** Shared scheduled-task thread pool (3 tasks per active region). */
    public record SchedulerProperties(
            @DefaultValue("12") int poolSize,
            @DefaultValue("booking-confirmation-scheduler-") String threadNamePrefix,
            @DefaultValue("10") int awaitTerminationSeconds) {}

    /** PROCESS stage: inbox -> transform -> outbox. */
    public record ProcessProperties(
            @DefaultValue("3000") long tickIntervalMs,
            @DefaultValue("200") int batchSize) {}

    /** RELAY stage: outbox -> Kafka. */
    public record RelayProperties(
            @DefaultValue("3000") long tickIntervalMs,
            @DefaultValue("100") int batchSize) {}

    /** REQUEUE stage: SEND_FAILURE -> NEW within retry budget. */
    public record RequeueProperties(
            @DefaultValue("60000") long tickIntervalMs,
            @DefaultValue("5") int maxRetries) {}

    /** Resilience4j circuit-breaker config for the relay (Kafka publish) stage. */
    public record ResilienceProperties(
            @DefaultValue("50") float failureRateThreshold,
            @DefaultValue("50") float slowCallRateThreshold,
            @DefaultValue("3000") long slowCallDurationThresholdMs,
            @DefaultValue("100") int slidingWindowSize,
            @DefaultValue("60000") long waitDurationInOpenStateMs,
            @DefaultValue("5") int permittedCallsInHalfOpenState,
            @DefaultValue("10") int minimumNumberOfCalls) {}

    public List<Region> activeRegions() {
        return regions.entrySet().stream()
                .filter(entry -> entry.getValue().active())
                .map(Map.Entry::getKey)
                .toList();
    }

    public RegionProperties region(final Region region) {
        return regions.get(region);
    }
}
