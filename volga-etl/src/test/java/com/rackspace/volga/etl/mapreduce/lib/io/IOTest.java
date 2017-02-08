package com.rackspace.volga.etl.mapreduce.lib.io;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 1:34 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class IOTest {

    @Test
    public void whenSettingIO() throws IOException {
        Job job = new Job();
        job.getConfiguration().set("mapreduce.output.dir","output");
        job.getConfiguration().set("mapreduce.input.path","input");
        IO.setOutput("text", job);
        IO.setInput("text", job);
        assertEquals("output", FileOutputFormat.getOutputPath(job).getName());
        assertEquals("input", FileInputFormat.getInputPaths(job)[0].getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSettingUnknownInput() throws IOException {
        Job job = new Job();
        IO.setInput("unknown", job);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenSettingUnknownOutput() throws IOException {
        Job job = new Job();
        IO.setOutput("unknown", job);
    }

}
