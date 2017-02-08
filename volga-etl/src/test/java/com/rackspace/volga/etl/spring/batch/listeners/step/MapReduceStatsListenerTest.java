package com.rackspace.volga.etl.spring.batch.listeners.step;

import com.rackspace.volga.etl.spring.TestConfig;
import com.rackspace.volga.etl.spring.batch.listeners.job.ETLJobListener;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:14 PM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)
public class MapReduceStatsListenerTest {

    @Autowired
    MapReduceStatsListener l;

    JobExecution jobExec;

    @Before
    public void init() {
        jobExec = PowerMockito.mock(JobExecution.class);
    }

    @Test
    public void whenNoMapReduceStepsArePresent() {
        l.statsListenerTemplate = PowerMockito.mock(JdbcTemplate.class);
        l.afterStep(new StepExecution("randomStep", jobExec, 1l));
        Mockito.verify(l.statsListenerTemplate, Mockito.times(0)).update(Mockito.any(PreparedStatementCreator.class));
    }

    @Test
    public void whenItShouldNotRun() {
        ReflectionTestUtils.setField(l, "shouldRun", false);
        l.statsListenerTemplate = PowerMockito.mock(JdbcTemplate.class);
        l.afterStep(new StepExecution("randomStep", jobExec, 1l));
        Mockito.verify(l.statsListenerTemplate, Mockito.times(0)).update(Mockito.any(PreparedStatementCreator.class));
    }

    @Test
    public void whenSavingToDb() {
        StepExecution s = new StepExecution("metricsMapReduce", jobExec, 1l);
        s.setStartTime(DateTime.now().minusDays(1).toDate());
        s.setEndTime(DateTime.now().toDate());
        s.getExecutionContext().put(ETLJobListener.MAP_INPUT_RECORDS_KEY, 10l);
        s.getExecutionContext().put(ETLJobListener.MAP_OUTPUT_RECORDS_KEY, 5l);
        l.afterStep(s);
    }

}
