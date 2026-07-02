package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;

class ApplicationBeansConfigTest {

    @Test
    void transactionTemplate_wrapsTheTransactionManager() {
        var config = new ApplicationBeansConfig();

        assertThat(config.transactionTemplate(mock(PlatformTransactionManager.class))).isNotNull();
    }
}
