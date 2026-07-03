package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.SchedulerProperties;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class SchedulingConfigTest {

    @Test
    void bookingConfirmationScheduler_isBuiltFromProperties() {
        var properties = new BookingConfirmationProperties(
                Map.of(), null, new SchedulerProperties(4, "test-scheduler-", 5), null, null, null, null);

        ThreadPoolTaskScheduler scheduler = new SchedulingConfig().bookingConfirmationScheduler(properties);

        try {
            assertThat(scheduler).isNotNull();
            assertThat(scheduler.getThreadNamePrefix()).isEqualTo("test-scheduler-");
        } finally {
            scheduler.shutdown();
        }
    }
}
