package com.bnpparibas.dec.bookingconfirmation.application.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Startup-time validation of the binding: misconfiguration must fail fast with a readable message,
 * not stall silently (batch-size 0), break Oracle IN-lists (batch-size > 1000), or NPE later
 * (missing topics/regions).
 */
class BookingConfirmationPropertiesValidationTest {

    private static final String[] VALID_MINIMUM = {
        "app.booking-confirmation.regions.AMER.internal-topic=internal.amer",
        "app.booking-confirmation.regions.AMER.published-topic=published.amer",
    };

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(PropertiesHost.class);

    @Test
    void shouldStart_withMinimalValidConfiguration() {
        runner.withPropertyValues(VALID_MINIMUM).run(context -> {
            assertThat(context).hasNotFailed();
            var properties = context.getBean(BookingConfirmationProperties.class);
            assertThat(properties.process().batchSize()).isEqualTo(200);
            assertThat(properties.relay().sendAwaitTimeoutMs()).isEqualTo(20000);
            assertThat(properties.requeue().processMaxRetries()).isEqualTo(3);
        });
    }

    @Test
    void shouldFailStartup_whenNoRegionsConfigured() {
        runner.run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailStartup_whenRegionTopicMissing() {
        runner.withPropertyValues("app.booking-confirmation.regions.AMER.internal-topic=internal.amer")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailStartup_whenBatchSizeIsZero() {
        runner.withPropertyValues(VALID_MINIMUM)
                .withPropertyValues("app.booking-confirmation.process.batch-size=0")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailStartup_whenBatchSizeExceedsOracleInListLimit() {
        runner.withPropertyValues(VALID_MINIMUM)
                .withPropertyValues("app.booking-confirmation.relay.batch-size=1001")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailStartup_whenLockTtlWouldBeSubSecond() {
        runner.withPropertyValues(VALID_MINIMUM)
                .withPropertyValues(
                        "app.booking-confirmation.process.tick-interval-ms=100",
                        "app.booking-confirmation.regions.AMER.lock-ttl-multiplier=2")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailStartup_whenConsumerRetryBudgetExceedsPollInterval() {
        runner.withPropertyValues(VALID_MINIMUM)
                .withPropertyValues(
                        "app.booking-confirmation.consumer.retry-backoff-ms=200000",
                        "app.booking-confirmation.consumer.retry-max-attempts=3")
                .run(context -> assertThat(context).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(BookingConfirmationProperties.class)
    static class PropertiesHost {}
}
