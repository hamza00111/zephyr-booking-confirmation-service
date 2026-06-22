package com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Single Oracle datasource + JDBC template + transaction manager.
 *
 * <p>Built explicitly from {@code spring.datasource.*} so the JDBC repositories share one
 * {@link NamedParameterJdbcTemplate} (qualified by {@link #ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE}) and
 * the scheduled stages share one {@link PlatformTransactionManager} for their tick transactions.
 */
@Configuration
@EnableTransactionManagement
public class ZephyrDataSourceConfiguration {

    public static final String ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE = "zephyrNamedParameterJdbcTemplate";
    public static final String ZEPHYR_TRANSACTION_MANAGER = "zephyrTransactionManager";

    @Bean
    @Primary
    public DataSource dataSource(
            @Value("${spring.datasource.url}") final String url,
            @Value("${spring.datasource.username}") final String username,
            @Value("${spring.datasource.password}") final String password,
            @Value("${spring.datasource.driver-class-name:oracle.jdbc.driver.OracleDriver}") final String driver,
            @Value("${spring.datasource.hikari.maximum-pool-size:10}") final int maximumPoolSize,
            @Value("${spring.datasource.hikari.minimum-idle:1}") final int minimumIdle,
            @Value("${spring.datasource.hikari.connection-timeout:30000}") final long connectionTimeout,
            @Value("${spring.datasource.hikari.idle-timeout:600000}") final long idleTimeout,
            @Value("${spring.datasource.hikari.keepalive-time:10000}") final long keepaliveTime,
            @Value("${spring.datasource.hikari.validation-timeout:5000}") final long validationTimeout,
            @Value("${spring.datasource.hikari.leak-detection-threshold:60000}") final long leakDetectionThreshold) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driver);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setIdleTimeout(idleTimeout);
        config.setKeepaliveTime(keepaliveTime);
        config.setValidationTimeout(validationTimeout);
        config.setLeakDetectionThreshold(leakDetectionThreshold);
        config.setPoolName("zephyr-booking-confirmation-pool");
        return new HikariDataSource(config);
    }

    @Bean(name = ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE)
    @Primary
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(final DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = ZEPHYR_TRANSACTION_MANAGER)
    @Primary
    public PlatformTransactionManager transactionManager(final DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
