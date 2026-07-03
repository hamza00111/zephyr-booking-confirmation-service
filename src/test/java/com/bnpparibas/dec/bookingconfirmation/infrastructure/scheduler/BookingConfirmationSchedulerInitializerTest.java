package com.bnpparibas.dec.bookingconfirmation.infrastructure.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.ProcessProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RelayProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RequeueProperties;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingProcessServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingRelayServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.application.registry.BookingRequeueServiceRegistry;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRelayService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationArguments;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class BookingConfirmationSchedulerInitializerTest {

    @Test
    void run_initializesRegistries_andSchedulesThreeTasksPerActiveRegion() {
        var properties = new BookingConfirmationProperties(
                Map.of(Region.AMER, new RegionProperties(true, "internal.amer", "published.amer")),
                null,
                null,
                new ProcessProperties(3000, 200),
                new RelayProperties(3000, 100, 20000),
                new RequeueProperties(60000, 5, 3),
                null);
        var processRegistry = mock(BookingProcessServiceRegistry.class);
        var relayRegistry = mock(BookingRelayServiceRegistry.class);
        var requeueRegistry = mock(BookingRequeueServiceRegistry.class);
        when(processRegistry.service(Region.AMER)).thenReturn(mock(BookingProcessService.class));
        when(relayRegistry.service(Region.AMER)).thenReturn(mock(BookingRelayService.class));
        when(requeueRegistry.service(Region.AMER)).thenReturn(mock(BookingRequeueService.class));
        var scheduler = mock(ThreadPoolTaskScheduler.class);
        when(scheduler.scheduleWithFixedDelay(any(Runnable.class), any(Duration.class)))
                .thenReturn(mock(ScheduledFuture.class));
        var initializer = new BookingConfirmationSchedulerInitializer(
                processRegistry, relayRegistry, requeueRegistry, properties, scheduler);

        initializer.run(mock(ApplicationArguments.class));

        verify(processRegistry).initialize();
        verify(relayRegistry).initialize();
        verify(requeueRegistry).initialize();
        verify(scheduler, times(3)).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

        initializer.shutdown();
    }
}
