package com.bnpparibas.dec.bookingconfirmation.application.config;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Root configuration bound from {@code app.booking-confirmation}.
 *
 * <p>Per-region settings live under {@code regions}; per-stage settings (process/relay/requeue) and
 * cross-cutting settings (consumer/scheduler/resilience) are global.
 */
@org.springframework.validation.annotation.Validated
@ConfigurationProperties(prefix = "app.booking-confirmation")
public record BookingConfirmationProperties(
        @NotEmpty Map<Region, @Valid RegionProperties> regions,
        @DefaultValue @Valid ConsumerProperties consumer,
        @DefaultValue @Valid SchedulerProperties scheduler,
        @DefaultValue @Valid ProcessProperties process,
        @DefaultValue @Valid RelayProperties relay,
        @DefaultValue @Valid RequeueProperties requeue,
        @DefaultValue @Valid ResilienceProperties resilience) {

    /** Per-region isolation unit: own topics, circuit breaker, scheduled tasks. */
    public record RegionProperties(
            @DefaultValue("true") boolean active,
            @NotBlank String internalTopic,
            @NotBlank String publishedTopic) {}

    /** Kafka consumer (inbox ingestion) tuning. */
    public record ConsumerProperties(
            @Min(1) @DefaultValue("3") int concurrency,
            @Min(1) @DefaultValue("500") int maxPollRecords,
            @Positive @DefaultValue("300000") int maxPollIntervalMs,
            @Positive @DefaultValue("2000") long retryBackoffMs,
            @Min(1) @DefaultValue("3") int retryMaxAttempts) {

        /** Blocking retries must finish well inside the poll interval or the consumer rebalances. */
        @AssertTrue(message = "consumer retry-backoff-ms * retry-max-attempts must be below max-poll-interval-ms")
        public boolean isRetryBudgetWithinPollInterval() {
            return retryBackoffMs * retryMaxAttempts < maxPollIntervalMs;
        }
    }

    /** Shared scheduled-task thread pool (3 tasks per active region). */
    public record SchedulerProperties(
            @Min(1) @DefaultValue("12") int poolSize,
            @DefaultValue("booking-confirmation-scheduler-") String threadNamePrefix,
            @DefaultValue("10") int awaitTerminationSeconds) {}

    /** PROCESS stage: inbox -> transform -> outbox. */
    public record ProcessProperties(
            @Positive @DefaultValue("3000") long tickIntervalMs,
            @Min(1) @Max(1000) @DefaultValue("200") int batchSize) {}

    /** RELAY stage: outbox -> Kafka. */
    public record RelayProperties(
            @Positive @DefaultValue("3000") long tickIntervalMs,
            @Min(1) @Max(1000) @DefaultValue("100") int batchSize,
            @Min(15000) @DefaultValue("20000") long sendAwaitTimeoutMs) {}

    /** REQUEUE stage: SEND_FAILURE -> NEW within retry budget. */
    public record RequeueProperties(
            @Positive @DefaultValue("60000") long tickIntervalMs,
            @Min(1) @DefaultValue("5") int maxRetries,
            @Min(1) @DefaultValue("3") int processMaxRetries) {}

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
