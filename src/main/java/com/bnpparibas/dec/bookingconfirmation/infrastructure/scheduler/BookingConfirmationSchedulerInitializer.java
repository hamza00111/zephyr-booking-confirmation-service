package com.bnpparibas.dec.bookingconfirmation.infrastructure.scheduler;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingProcessServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingRelayServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingRequeueServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Initializes the three stage registries and schedules a {@code fixedDelay} PROCESS, RELAY, and
 * REQUEUE task for each active region in the shared scheduler.
 *
 * <p>Every task is fully region-bound; each tick self-guards on the pause flag and the per-region
 * distributed lock. On shutdown all futures are cancelled (in-flight ticks finish) and all locks are
 * released so peers can take over immediately.
 */
@Component
@Slf4j
public class BookingConfirmationSchedulerInitializer implements ApplicationRunner {

    private final BookingProcessServiceRegistry processRegistry;
    private final BookingRelayServiceRegistry relayRegistry;
    private final BookingRequeueServiceRegistry requeueRegistry;
    private final BookingConfirmationProperties properties;
    private final DistributedLockRepository lockRepository;
    private final InstanceId instanceId;
    private final ThreadPoolTaskScheduler scheduler;
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    public BookingConfirmationSchedulerInitializer(
            final BookingProcessServiceRegistry processRegistry,
            final BookingRelayServiceRegistry relayRegistry,
            final BookingRequeueServiceRegistry requeueRegistry,
            final BookingConfirmationProperties properties,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId,
            @Qualifier("bookingConfirmationScheduler") final ThreadPoolTaskScheduler scheduler) {
        this.processRegistry = processRegistry;
        this.relayRegistry = relayRegistry;
        this.requeueRegistry = requeueRegistry;
        this.properties = properties;
        this.lockRepository = lockRepository;
        this.instanceId = instanceId;
        this.scheduler = scheduler;
    }

    @Override
    public void run(final ApplicationArguments args) {
        processRegistry.initialize();
        relayRegistry.initialize();
        requeueRegistry.initialize();

        final long processMs = properties.process().tickIntervalMs();
        final long relayMs = properties.relay().tickIntervalMs();
        final long requeueMs = properties.requeue().tickIntervalMs();

        for (final Region region : properties.activeRegions()) {
            schedule(processRegistry.service(region), processMs);
            schedule(relayRegistry.service(region), relayMs);
            schedule(requeueRegistry.service(region), requeueMs);
        }
        log.info("Scheduled {} task(s) across {} active region(s) for instanceId={}",
                scheduledFutures.size(), properties.activeRegions().size(), instanceId);
    }

    private void schedule(final BookingConfirmationService service, final long delayMs) {
        final ScheduledFuture<?> future =
                scheduler.scheduleWithFixedDelay(() -> runTick(service), Duration.ofMillis(delayMs));
        scheduledFutures.add(future);
        log.info("[{}] Scheduled fixedDelay={}ms", service.processIdentifier(), delayMs);
    }

    private void runTick(final BookingConfirmationService service) {
        try {
            service.tick();
        } catch (final RuntimeException exception) {
            log.error("[{}] Scheduled tick failed", service.processIdentifier(), exception);
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduledFutures.forEach(future -> future.cancel(false));
        lockRepository.releaseAll(instanceId);
        log.info("Cancelled {} scheduled task(s) and released locks for instanceId={}",
                scheduledFutures.size(), instanceId);
    }
}
