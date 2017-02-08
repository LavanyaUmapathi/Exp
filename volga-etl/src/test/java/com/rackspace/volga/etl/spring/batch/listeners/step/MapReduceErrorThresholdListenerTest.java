package com.rackspace.volga.etl.spring.batch.listeners.step;

import com.rackspace.volga.etl.spring.batch.listeners.job.ETLJobListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * User: alex.silva
 * Date: 9/8/14
 * Time: 12:31 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class MapReduceErrorThresholdListenerTest {

    private MapReduceErrorThresholdListener l;

    private StepExecution exec;

    @Before
    public void setUp() {
        l = new MapReduceErrorThresholdListener();
        ReflectionTestUtils.setField(l, "errorThreshold", .1);
        ReflectionTestUtils.setField(l, "shouldFailJobWhenThresholdExceeded", true);
        exec = new StepExecution("SomeMapReduce", new JobExecution(1l));
        exec.getExecutionContext().put(ETLJobListener.MAP_INPUT_RECORDS_KEY, 10L);
        exec.getExecutionContext().put(ETLJobListener.MAP_OUTPUT_RECORDS_KEY, 1L);
        exec.getExecutionContext().put(ETLJobListener.REDUCE_INPUT_RECORDS_KEY, 1L);
    }

    @Test
    public void whenNoMapReduceStep() {
        StepExecution s = new StepExecution("SomeStep", new JobExecution(1l));
        s.setExitStatus(ExitStatus.NOOP);
        Assert.assertEquals(ExitStatus.NOOP, l.afterStep(s));
    }

    @Test
    public void whenMapReduceStepFail() {
         Assert.assertEquals(ExitStatus.FAILED, l.afterStep(exec));
    }

    @Test
    public void whenMapReduceStepNoFail() {
        ReflectionTestUtils.setField(l, "shouldFailJobWhenThresholdExceeded", false);
        exec.setExitStatus(ExitStatus.COMPLETED);
        Assert.assertEquals(ExitStatus.COMPLETED, l.afterStep(exec));
    }

}
