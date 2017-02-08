package com.rackspace.volga.etl.spring.batch.listeners.job;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rackspace.volga.etl.spring.TestConfig;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.springframework.batch.core.*;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;

import java.util.Map;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:14 PM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)
@TestExecutionListeners(listeners = {DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class, TransactionalTestExecutionListener.class})
public class VolgaETLJobListenerTest {

    @Autowired
    VolgaETLJobListener l;

    JobExecution exec;

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final String END_TIME = "2010-08-11T14:11:52.225Z";

    private static final String START_TIME = "2000-08-11T14:11:52.225Z";

    @Before
    public void init() {
        exec = PowerMockito.mock(JobExecution.class);
        Mockito.when(exec.getExitStatus()).thenReturn(new ExitStatus("COMPLETED", "Exit description"));
        JobInstance instance = PowerMockito.mock(JobInstance.class);

        ExecutionContext jctx = new ExecutionContext();
        Mockito.when(exec.getExecutionContext()).thenReturn(jctx);

        //configuration name
        Mockito.when(exec.getJobConfigurationName()).thenReturn("CONFIG NAME");

        //job instance
        Mockito.when(exec.getJobInstance()).thenReturn(instance);
        Mockito.when(instance.getJobName()).thenReturn("job name");

        //start/end times
        Mockito.when(exec.getEndTime()).thenReturn(DateTime.parse(END_TIME, FORMATTER).withZone(DateTimeZone.UTC)
                .toDate());
        Mockito.when(exec.getStartTime()).thenReturn(DateTime.parse(START_TIME, FORMATTER).withZone(DateTimeZone.UTC)
                .toDate());

        //parameters
        JobParameters params = new JobParameters(ImmutableMap.of("PARAM1", new JobParameter("VAL1")));
        Mockito.when(exec.getJobParameters()).thenReturn(params);

        //exceptions
        Mockito.when(exec.getFailureExceptions()).thenReturn(Lists.<Throwable>newArrayList(new RuntimeException("Fake" +
                " Exception")));

        Mockito.reset(l.getIncidentReporter());
    }

    @Test
    public void whenNoMapReduceStepsArePresent() {
        Mockito.when(exec.getStepExecutions()).thenReturn(Lists.<StepExecution>newArrayList());
        l.afterJob(exec);
        Mockito.verify(l.getIncidentReporter(), Mockito.times(0)).createIncident(Mockito.anyString(), Mockito.anyMap());
    }

    @Test
    public void whenReportingFailedJob() {
        Mockito.when(exec.getExitStatus()).thenReturn(ExitStatus.FAILED);
        l.afterJob(exec);
        Map<String, String> details = Maps.newHashMap();
        details.put("Job Name", "job name");
        details.put("Job Start Time", START_TIME);
        details.put("Job End Time", END_TIME);
        details.put("Job Parameter PARAM1", "VAL1");
        details.put("Job Configuration Name", exec.getJobConfigurationName());
        details.put("Job Exit Code", "FAILED");
        details.put("Job Exit Description", exec.getExitStatus().getExitDescription());
        details.put("Job Exception 1", "Fake Exception");
        Mockito.verify(l.getIncidentReporter(), Mockito.times(1)).createIncident("Volga ETL::job name", details);
    }


    @Test
    public void whenReportingIncident() {
        StepExecution s = new StepExecution("metricsMapReduce", exec, 1l);
        Map sdetails = Maps.newHashMap();
        sdetails.put("Current threshold", "0.1");
        sdetails.put("Error ratio", "0.5");
        s.getExecutionContext().put(ETLJobListener.INCIDENT_KEY_PREFIX + "VolgaTest", sdetails);
        s.getExecutionContext().put(ETLJobListener.MAP_INPUT_RECORDS_KEY, 10l);
        s.getExecutionContext().put(ETLJobListener.MAP_OUTPUT_RECORDS_KEY, 5l);
        Mockito.when(exec.getStepExecutions()).thenReturn(Lists.newArrayList(s));

        l.afterJob(exec);

        Map<String, String> details = Maps.newHashMap();
        details.put("Job Name", "job name");
        details.put("Job Start Time", START_TIME);
        details.put("Job End Time", END_TIME);
        details.put("Job Parameter PARAM1", "VAL1");
        details.put("Job Configuration Name", exec.getJobConfigurationName());
        details.put("Job Exit Code", "COMPLETED");
        details.put("Job Exit Description", exec.getExitStatus().getExitDescription());
        details.put("Job Exception 1", "Fake Exception");
        details.put("Step Reporting Error", "VolgaTest");
        details.putAll(sdetails);
        Mockito.verify(l.getIncidentReporter(), Mockito.times(1)).createIncident("Volga ETL::job name", details);
    }


    @Test
    public void whenNotReportingIncident() {
        StepExecution s = new StepExecution("metricsMapReduce", exec, 1l);
        s.getExecutionContext().put(ETLJobListener.MAP_INPUT_RECORDS_KEY, 10l);
        s.getExecutionContext().put(ETLJobListener.MAP_OUTPUT_RECORDS_KEY, 10l);
        Mockito.when(exec.getStepExecutions()).thenReturn(Lists.newArrayList(s));
        l.afterJob(exec);

        Mockito.verify(l.getIncidentReporter(), Mockito.times(0)).createIncident(Mockito.anyString(), Mockito.anyMap());
    }

}
