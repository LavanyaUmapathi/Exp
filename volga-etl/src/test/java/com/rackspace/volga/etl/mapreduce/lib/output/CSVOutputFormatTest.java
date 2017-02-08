package com.rackspace.volga.etl.mapreduce.lib.output;

import com.rackspace.volga.etl.mapreduce.lib.io.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * User: alex.silva
 * Date: 7/29/14
 * Time: 12:02 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class CSVOutputFormatTest {
    CSVOutputFormat f;

    Configuration conf;

    @Before
    public void setup() throws IOException {
        final FileSystem fs = PowerMockito.mock(FileSystem.class);

        final Path path = new Path("fake-path") {
            @Override
            public FileSystem getFileSystem(Configuration conf) {
                return fs;
            }
        };
        f = new CSVOutputFormat() {
            @Override
            public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                return path;
            }
        };
        conf = new Configuration(false);
        conf.set("mapreduce.output.dir", "output");
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("mapreduce.input.path", "/dev/null/input");
    }

    @Test
    public void whenGettingRecordWriterUncompressed() throws IOException, InterruptedException {

        RecordWriter writer = f.getRecordWriter(new TaskAttemptContextImpl(conf,
                new TaskAttemptID("id", 1, false, 1, 2)));
        Assert.assertNotNull(writer);

    }


    @Test
    public void whenWriting() throws IOException, InterruptedException {
        conf.set("csvoutputformat.token.delimiter", ";");
        CSVOutputFormat.CSVRecordWriter writer = (CSVOutputFormat.CSVRecordWriter) f.getRecordWriter(new
                TaskAttemptContextImpl(conf, new TaskAttemptID("id", 1, false, 1, 2)));
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writer.out = new DataOutputStream(b);
        writer.write(new TextArrayWritable(new Text[]{new Text("field1"), new Text("field2"), new Text("field3")}),
                NullWritable.get());
        writer.out.flush();
        String output = b.toString();
        Assert.assertEquals("field1;field2;field3\n", output);
    }

    @Test
    public void whenWritingCompressed() throws IOException, InterruptedException {
        conf.set("csvoutputformat.token.delimiter", ";");
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", DummyCodec.class, CompressionCodec.class);
        CSVOutputFormat.CSVRecordWriter writer = (CSVOutputFormat.CSVRecordWriter) f.getRecordWriter(new
                TaskAttemptContextImpl(conf, new TaskAttemptID("id", 1, false, 1, 2)));
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writer.out = new DataOutputStream(b);
        writer.write(new TextArrayWritable(new Text[]{new Text("field1"), new Text("field2"), new Text("field3")}),
                NullWritable.get());
        writer.out.flush();
        String output = b.toString();
        Assert.assertEquals("field1;field2;field3\n", output);
    }


    @Test
    public void whenWritingWithSeparator() throws IOException, InterruptedException {
        CSVOutputFormat.CSVRecordWriter writer = (CSVOutputFormat.CSVRecordWriter) f.getRecordWriter(new
                TaskAttemptContextImpl(conf, new TaskAttemptID("id", 1, false, 1, 2)));
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writer.out = new DataOutputStream(b);
        writer.write(new TextArrayWritable(new Text[]{new Text("field1,field11"), new Text("field2"), new Text("field3")}),
                NullWritable.get());

        writer.out.flush();
        String output = b.toString();
        Assert.assertEquals("\"field1,field11\",field2,field3\n", output);
    }

    @Test
    public void whenGettingRecordWriterCompressed() throws IOException, InterruptedException {
        conf.setBoolean("mapreduce.output.compress", true);
        conf.setClass("mapreduce.output.compression.codec", DummyCodec.class, CompressionCodec.class);
        CSVOutputFormat.CSVRecordWriter writer = (CSVOutputFormat.CSVRecordWriter) f.getRecordWriter(new
                TaskAttemptContextImpl(conf, new TaskAttemptID("id", 1, false, 1, 2)));
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writer.out = new DataOutputStream(b);
        writer.write(new TextArrayWritable(new Text[]{new Text("field1"), new Text("field2"), new Text("field3")}),
                NullWritable.get());
        writer.out.flush();
        String output = b.toString();
        Assert.assertEquals("field1,field2,field3\n", output);

    }

    public static class DummyCodec extends DefaultCodec {
        @Override
        public CompressionOutputStream createOutputStream(OutputStream out) {
            CompressionOutputStream c = PowerMockito.mock(CompressionOutputStream.class);
            return c;
        }
    }


}
