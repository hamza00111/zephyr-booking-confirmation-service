package com.bnpparibas.dec.bookingconfirmation;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(BookingConfirmationProperties.class)
public class ZephyrBookingConfirmationApplication {

    public static void main(final String[] args) {
        SpringApplication.run(ZephyrBookingConfirmationApplication.class, args);
    }
}
