package com.bnpparibas.dec.bookingconfirmation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

class ZephyrBookingConfirmationApplicationTest {

    @Test
    void main_delegatesToSpringApplicationRun() {
        try (var springApplication = mockStatic(SpringApplication.class)) {
            String[] args = {"--server.port=0"};

            ZephyrBookingConfirmationApplication.main(args);

            springApplication.verify(
                    () -> SpringApplication.run(eq(ZephyrBookingConfirmationApplication.class), any(String[].class)));
        }
    }

    @Test
    void applicationClass_canBeInstantiated() {
        assertThat(new ZephyrBookingConfirmationApplication()).isNotNull();
    }
}
