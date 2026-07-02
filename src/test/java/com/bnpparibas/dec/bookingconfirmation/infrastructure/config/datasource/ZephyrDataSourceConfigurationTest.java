package com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class ZephyrDataSourceConfigurationTest {

    private final ZephyrDataSourceConfiguration config = new ZephyrDataSourceConfiguration();
    private final DataSource dataSource = mock(DataSource.class);

    @Test
    void namedParameterJdbcTemplate_wrapsTheDataSource() {
        assertThat(config.namedParameterJdbcTemplate(dataSource)).isNotNull();
    }

    @Test
    void transactionManager_wrapsTheDataSource() {
        assertThat(config.transactionManager(dataSource)).isNotNull();
    }
}
