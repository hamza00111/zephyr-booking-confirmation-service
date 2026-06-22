package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Shared singletons: this JVM's {@link InstanceId} and a {@link TransactionTemplate} for the stages. */
@Configuration
public class ApplicationBeansConfig {

    @Bean
    public InstanceId instanceId() {
        return InstanceId.generate();
    }

    @Bean
    public TransactionTemplate transactionTemplate(final PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
