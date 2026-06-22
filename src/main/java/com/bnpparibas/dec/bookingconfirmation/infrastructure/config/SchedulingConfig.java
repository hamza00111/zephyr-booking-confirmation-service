package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.SchedulerProperties;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Shared {@link ThreadPoolTaskScheduler} backing the 3 fixed-delay tasks per active region.
 *
 * <p>Sized for {@code 3 * activeRegions}; graceful shutdown waits for in-flight ticks. Tasks are
 * registered programmatically by the scheduler initializer (no {@code @Scheduled} annotations).
 */
@Configuration
public class SchedulingConfig {

    @Bean(name = "bookingConfirmationScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler bookingConfirmationScheduler(final BookingConfirmationProperties properties) {
        final SchedulerProperties schedulerProperties = properties.scheduler();
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(schedulerProperties.poolSize());
        scheduler.setThreadNamePrefix(schedulerProperties.threadNamePrefix());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(schedulerProperties.awaitTerminationSeconds());
        scheduler.setErrorHandler(throwable -> LoggerFactory.getLogger(SchedulingConfig.class)
                .error("Unhandled error in a booking confirmation scheduled task", throwable));
        scheduler.initialize();
        return scheduler;
    }
}
