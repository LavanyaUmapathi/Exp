package com.rackspace.volga.etl.mapreduce.lib.output;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 1:56 PM
 * Copyright Rackspace Hosting, Inc.
 */

public class TextOutputConfigTest {
    TextOutputConfig cfg;

    @Before
    public void setup() throws Exception {
        cfg = PowerMockito.spy(new TextOutputConfig());
        Path path = PowerMockito.mock(Path.class);
        PowerMockito.whenNew(Path.class).withAnyArguments().thenReturn(path);
    }

    @Test
    public void whenGettingName() {
        assertEquals("text", cfg.getName());
    }

    @Test
    public void whenGettingFormatClass() {
        assertEquals(TextOutputFormat.class, cfg.getOutputFormatClass());

    }

    @Test(expected = IllegalArgumentException.class)
    public void whenConfiguringWithBlankPath() throws IOException, ParseException, ClassNotFoundException {
        Job job = new Job();
        cfg.configure(job);

        assertEquals("/dev/null", FileOutputFormat.getOutputPath(job));
        assertEquals(Text.class, job.getOutputKeyClass());
        assertEquals(Text.class, job.getOutputValueClass());
        assertEquals(TextOutputFormat.class, job.getOutputFormatClass());
    }

}
