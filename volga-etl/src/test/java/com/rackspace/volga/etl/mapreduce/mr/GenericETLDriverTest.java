package com.rackspace.volga.etl.mapreduce.mr;

import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import com.rackspace.volga.etl.utils.ExitException;
import com.rackspace.volga.etl.utils.NoExitSecurityManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareOnlyThisForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 11:28 AM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.logging.log4j.core.jmx.*")
@PrepareOnlyThisForTest({FileSystem.class, IO.class, GenericETLDriver.class, LazyOutputFormat.class})
public class GenericETLDriverTest {
    GenericETLDriver driver;

    String[] args = new String[]{
            "volga.jar", "Driver", "-o", "/dev/null", "-i", "input", "-mapreduce.output.format", "text",
            "-mapreduce.reducer.class", "com.rackspace.volga.etl.mapreduce.mr.GenericETLReducer"
    };


    @Before
    public void setUp() throws Exception {
        System.setSecurityManager(new NoExitSecurityManager());
        driver = PowerMockito.spy(new GenericETLDriver());
        PowerMockito.doReturn(new Configuration()).when(driver, "getConf");
        Configuration conf = new Configuration();
        Job job = PowerMockito.mock(Job.class);
        PowerMockito.whenNew(Job.class).withAnyArguments().thenReturn(job);
        PowerMockito.when(job.getConfiguration()).thenReturn(conf);
        PowerMockito.mockStatic(IO.class, LazyOutputFormat.class);
    }

    @After
    public void tearDown() {
        System.setSecurityManager(null);
    }

    @Test(expected = ExitException.class)
    public void testMainMethod() throws Exception {
        driver.main(args);
    }

    @Test
    public void whenRunningGenericDriver() throws Exception {
        String[] args = new String[]{
                "volga.jar", "Driver", "-o", "output", "-i", "input", "-mapreduce.output.format", "text"
        };
        Assert.assertEquals(1, driver.run(args));
    }

    @Test
    public void whenRunningGenericDriverWithReducerClass() throws Exception {
        Assert.assertEquals(1, driver.run(args));
    }

    @Test
    public void whenRunningGenericDriverWithNoClass() throws Exception {
        String[] rargs = new String[]{
                "volga.jar", "Driver", "-o", "output", "-i", "input", "-mapreduce.output.format", "text",
                "-mapreduce.reducer.class", "none"
        };
        Assert.assertEquals(1, driver.run(rargs));
    }

}
