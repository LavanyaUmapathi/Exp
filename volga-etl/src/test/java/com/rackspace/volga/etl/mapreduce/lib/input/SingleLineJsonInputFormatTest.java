package com.rackspace.volga.etl.mapreduce.lib.input;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.Matchers.equalTo;

/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 11:45 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class SingleLineJsonInputFormatTest {

    private static final String FAKE_PATH = "/fake";

    private SingleLineJsonInputFormat jif;

    private SingleLineJsonInputFormat.JsonRecordReader reader;

    @Before
    public void init() throws IOException, InterruptedException {
        jif = new SingleLineJsonInputFormat();
        reader = new SingleLineJsonInputFormat.JsonRecordReader();
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        File testFile = new File(Thread.currentThread().getContextClassLoader().getResource("maas/metrics.json")
                .getFile());
        FileSplit split = new FileSplit(new Path(testFile.getAbsolutePath()), 0, testFile.getTotalSpace(),
                new String[]{});
        reader.initialize(split, new TaskAttemptContextImpl(new Configuration(), new TaskAttemptID()));
    }

    @After
    public void cleanUp() throws IOException {
        reader.close();
    }

    @Test
    public void whenCreatingRecordReader() {
        Assert.assertEquals(SingleLineJsonInputFormat.JsonRecordReader.class,
                jif.createRecordReader(Mockito.mock(FileSplit.class), Mockito.mock(TaskAttemptContext.class)).getClass()
        );
    }

    @Test
    public void shouldBeSplittable() {
        Assert.assertTrue(jif.isSplitable(new JobContextImpl(new Configuration(), new JobID()), new Path(FAKE_PATH,
                FAKE_PATH)));
    }

    @Test
    public void shouldNotBeSplittable() {
        Path path = Mockito.mock(Path.class);
        Mockito.when(path.getName()).thenReturn("name.bz2");
        Assert.assertFalse(jif.isSplitable(new JobContextImpl(new Configuration(), new JobID()), path));
    }

    @Test
    public void whenGettingCurrentKey() throws IOException, InterruptedException {
        List<String> jsons = Files.readLines(new ClassPathResource("maas/metrics.json").getFile(),
                Charset.defaultCharset());
        Assert.assertNull(reader.getCurrentKey());
        reader.nextKeyValue();
        Assert.assertEquals(new LongWritable(0), reader.getCurrentKey());
        reader.nextKeyValue();
        Assert.assertEquals(new LongWritable(jsons.get(0).length() + 1), reader.getCurrentKey());
    }

    @Test
    public void whenGettingCurrentValue() throws IOException, InterruptedException {
        MapWritable value = reader.getCurrentValue();
        Assert.assertEquals(0, value.size());
        reader.nextKeyValue();
        Assert.assertEquals(14, value.size());
    }

    @Test
    public void whenGettingProgress() throws IOException, InterruptedException {
        Assert.assertEquals(0d, reader.getProgress(), 0);
        reader.nextKeyValue();
        Assert.assertEquals(2.81e-9d, reader.getProgress(), 0.01);
    }

    @Test
    public void whenParsingJson() throws IOException, InterruptedException {
        reader.nextKeyValue();
        MapWritable value = reader.getCurrentValue();
        Assert.assertEquals(new Text("1"), value.get(new Text("version")));
        Assert.assertEquals(new Text("1395941810615"),
                value.get(new Text("timestamp")));
        Assert.assertEquals(new Text("ac4ubIxzmW"), value.get(new Text("accountId")));
        Assert.assertEquals(new Text("898450"), value.get(new Text("tenantId")));
        Assert.assertEquals(new Text("enC6anIcNY"), value.get(new Text("entityId")));
        Assert.assertEquals(new Text("agent.load_average"), value.get(new Text("checkType")));

        String metrics = value.get(new Text("metrics")).toString();
        with(metrics).assertThat("$.15m.metricType", equalTo(110));
        with(metrics).assertThat("$.5m.metricType", equalTo(110));
    }
}
