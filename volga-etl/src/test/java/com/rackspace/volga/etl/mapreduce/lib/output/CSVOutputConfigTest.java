package com.rackspace.volga.etl.mapreduce.lib.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.nio.file.Paths;

/**
 * User: alex.silva
 * Date: 7/29/14
 * Time: 11:24 AM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CSVOutputConfig.class})
@PowerMockIgnore({"javax.management*", "javax.xml.*", "org.w3c.*", "org.apache.hadoop.*", "com.sun.*",
        "org.apache.logging.log4j.*"})
public class CSVOutputConfigTest {
    CSVOutputConfig cfg;

    Job job;

    @Before
    public void setup() throws Exception {
        cfg = PowerMockito.spy(new CSVOutputConfig());
        job = new Job(new Configuration(false));
    }

    @Test
    public void whenGettingName() {
        Assert.assertEquals(cfg.getName(), "csv");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUsingNoOutputPath()throws Exception  {
        cfg.configure(job);
    }


    @Test
    public void whenConfiguring() throws Exception {
        String outputDirAbs = "file:" + Paths.get("output").toAbsolutePath().normalize().toString();
        job.getConfiguration().set("mapreduce.output.dir", "output");
        cfg.configure(job);
        Assert.assertEquals(job.getOutputKeyClass(), Text.class);
        Assert.assertEquals(job.getOutputValueClass(), Text.class);
        Assert.assertEquals(job.getOutputFormatClass(), CSVOutputFormat.class);
        Assert.assertEquals(CSVOutputFormat.getOutputPath(job), new Path(outputDirAbs));
    }
}
