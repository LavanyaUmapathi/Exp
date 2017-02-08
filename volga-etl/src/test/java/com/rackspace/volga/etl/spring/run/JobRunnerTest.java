package com.rackspace.volga.etl.spring.run;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:59 PM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobRunner.class})
@PowerMockIgnore("javax.management.*")
public class JobRunnerTest {
    JobRunner runner;

    @Test
    public void testMain() throws Exception {
//        JobRunner launcher = PowerMockito.mock(JobRunner.class);
//        PowerMockito.whenNew(JobRunner.class).withNoArguments().thenReturn(launcher);
//        PowerMockito.mockStatic(SpringApplication.class);
//        JobRunner.main(new String[]{"test1", "test2"});
//        Mockito.verify(launcher).run(new String[]{"test1", "test2"});
    }


}
