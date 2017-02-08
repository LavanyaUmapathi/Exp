package com.rackspace.volga.etl.mapreduce.lib.input;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import com.rackspace.volga.etl.mapreduce.lib.selectors.Selector;
import com.rackspace.volga.etl.mapreduce.lib.selectors.Selectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;

/**
 * User: alex.silva
 * Date: 7/29/14
 * Time: 10:50 AM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Selectors.class)
@PowerMockIgnore({"javax.management*", "javax.xml.*", "org.w3c.*", "org.apache.hadoop.*", "com.sun.*"})
public class TextInputConfigTest {
    TextInputConfig cfg;

    Job job;

    @Before
    public void init() throws Exception {
        cfg = new TextInputConfig();
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("mapreduce.input.path", "/dev/null/input");
        Path selectedPath = new Path("/selectedPath");
        job = new Job(conf);
        PowerMockito.mockStatic(Selectors.class);
        Selector selector = PowerMockito.mock(Selector.class);
        PowerMockito.when(selector.select(job, selectedPath, 0)).thenReturn(Lists.newArrayList(new FileStatus(10l,
                true, 10, 10l, 40l, selectedPath)));
        PowerMockito.when(Selectors.class, "get", "dummySelector").thenReturn(selector);
    }

    @Before
    public void setup() {
        cfg = new TextInputConfig();
    }

    @Test
    public void whenGettingName() {
        Assert.assertEquals(cfg.getName(), "text");
    }

    @Test
    public void whenDoingSimpleConfig() throws IOException {
        cfg.configure(job);
        Path[] paths = TextInputFormat.getInputPaths(job);
        Assert.assertArrayEquals(paths, new Path[]{new Path("file:/dev/null/input")});
    }

    @Test
    public void whenDoingConfigWithInputFilePath() throws IOException {
        job.getConfiguration().unset("mapreduce.input.path");
        File f = File.createTempFile("input", "test");
        CharSink sink = Files.asCharSink(f, Charsets.UTF_8);
        sink.writeLines(Lists.newArrayList("file:/dev/null/input1", "file:/dev/null/input2"));
        job.getConfiguration().set("mapreduce.input.path.file", f.getAbsolutePath());
        cfg.configure(job);
        Path[] paths = TextInputFormat.getInputPaths(job);
        Assert.assertArrayEquals(paths, new Path[]{new Path("file:/dev/null/input1"),
                new Path("file:/dev/null/input2")});
    }

    @Test
    public void whenDoingMixedConfig() throws IOException {
        File f = File.createTempFile("input", "test");
        CharSink sink = Files.asCharSink(f, Charsets.UTF_8);
        sink.writeLines(Lists.newArrayList("file:/dev/null/input1", "file:/dev/null/input2"));
        job.getConfiguration().set("mapreduce.input.path.file", f.getAbsolutePath());
        cfg.configure(job);
        Path[] paths = TextInputFormat.getInputPaths(job);
        Assert.assertArrayEquals(paths, new Path[]{
                new Path("file:/dev/null/input"),
                new Path("file:/dev/null/input1"),
                new Path("file:/dev/null/input2")});
    }

    @Test
    public void whenDoingConfigWithPattern() throws IOException {
        job.getConfiguration().set("mapreduce.input.pattern", "pattern");
        cfg.configure(job);
        Path[] paths = TextInputFormat.getInputPaths(job);
        Assert.assertArrayEquals(paths, new Path[]{new Path("file:/dev/null/input")});
        Assert.assertEquals(TextInputFormat.getInputPathFilter(job).getClass(), RegexPathFilter.class);
        Assert.assertEquals(job.getConfiguration().get(IO.INPUT_PATTERN), "pattern");
    }

    @Test
    public void whenDoingConfigWithSelector() throws IOException {
        job.getConfiguration().set("mapreduce.input.selector", "dummySelector");
        job.getConfiguration().set("mapreduce.input.path", "/selectedPath");
        cfg.configure(job);
        Path[] paths = TextInputFormat.getInputPaths(job);
        Assert.assertArrayEquals(paths, new Path[]{new Path("file:/selectedPath")});
    }

}
