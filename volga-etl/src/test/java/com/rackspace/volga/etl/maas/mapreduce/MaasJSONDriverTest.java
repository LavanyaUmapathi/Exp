package com.rackspace.volga.etl.maas.mapreduce;

import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import com.rackspace.volga.etl.mapreduce.mr.GenericETLDriver;
import com.rackspace.volga.etl.utils.ExitException;
import com.rackspace.volga.etl.utils.NoExitSecurityManager;
import org.apache.hadoop.conf.Configuration;
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
 * Time: 1:13 PM
 * Copyright Rackspace Hosting, Inc.
 */

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.logging.log4j.core.jmx.*")
@PrepareOnlyThisForTest({IO.class, GenericETLDriver.class, LazyOutputFormat.class})
public class MaasJSONDriverTest {
    MaasJSONDriver driver;

    String[] args = new String[]{
            "volga.jar", "Driver", "-o", "output", "-i", "input", "-mapreduce.output.format", "text",
            "-volga.etl.maas.event.type", "metric"
    };

    @Before
    public void setUp() throws Exception {
        System.setSecurityManager(new NoExitSecurityManager());
        driver = PowerMockito.spy(new MaasJSONDriver());
        Configuration conf = new Configuration();
        PowerMockito.doReturn(conf).when(driver, "getConf");
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
    public void whenSettingEventType() throws Exception {

        Assert.assertEquals(1, driver.run(args));
        Assert.assertEquals(Metric.class.getName(), driver.getConf().get(ETLConstants.DTO_CLASS_KEY));
    }
}
