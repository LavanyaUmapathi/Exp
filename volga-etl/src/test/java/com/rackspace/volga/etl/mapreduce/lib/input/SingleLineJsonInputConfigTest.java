package com.rackspace.volga.etl.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 1:53 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class SingleLineJsonInputConfigTest {

    @Test
    public void whenConfiguring() throws IOException, ClassNotFoundException {
        Job job = new Job();
        new SingleLineJsonInputConfig().configure(job);
        Assert.assertEquals(SingleLineJsonInputFormat.class, job.getInputFormatClass());
    }
}
