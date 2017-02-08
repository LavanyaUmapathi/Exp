package com.rackspace.volga.etl.spring;

import com.rackspace.volga.etl.spring.batch.listeners.job.VolgaETLJobListener;
import com.rackspace.volga.etl.spring.batch.listeners.step.MapReduceStatsListener;
import com.rackspace.volga.etl.spring.reporting.PagerDutyIncidentReporter;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:21 PM
 * Copyright Rackspace Hosting, Inc.
 */
@Configuration
@PropertySource("classpath:/test-volga-etl.properties")
public class TestConfig {
    @Bean
    public VolgaETLJobListener mapReduceBadRecordsListener() {
        VolgaETLJobListener listener = new VolgaETLJobListener();
        return listener;
    }

    @Bean
    public MapReduceStatsListener mapReduceStatsListener() {
        return new MapReduceStatsListener();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public PagerDutyIncidentReporter pagerDuty() {
        return PowerMockito.mock(PagerDutyIncidentReporter.class);
    }

    @Bean(name = "realPagerDuty")
    public PagerDutyIncidentReporter realPagerDuty() {
        return new PagerDutyIncidentReporter();
    }

    @Bean
    public JdbcTemplate jdbcTemplate() throws SQLException {
        DataSource ds = PowerMockito.mock(DataSource.class);
        Connection conn = PowerMockito.mock(Connection.class);
        Mockito.when(ds.getConnection()).thenReturn(conn);
        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(PowerMockito.mock(PreparedStatement.class));
        return new JdbcTemplate(ds);
    }
}
